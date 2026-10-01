"""
Scribble multiplayer game server.
FastAPI + WebSockets. In-memory state, no database.
Deployable on Render: uvicorn server:app --host 0.0.0.0 --port $PORT
"""
import asyncio
import json
import random
import string
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="Scribble Game")

# ---- Static files ----
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ---------------- Words ----------------
WORDS = [
    "apple", "banana", "cat", "dog", "house", "tree", "car", "sun", "moon",
    "star", "fish", "bird", "book", "chair", "table", "flower", "cloud",
    "rain", "snow", "mountain", "river", "boat", "train", "plane", "rocket",
    "pizza", "burger", "cake", "coffee", "guitar", "piano", "drum", "phone",
    "computer", "camera", "clock", "gift", "heart", "smile", "ghost", "robot",
    "dragon", "castle", "pirate", "ninja", "zombie", "rainbow", "volcano",
    "elephant", "giraffe", "penguin", "dolphin", "butterfly", "spider",
    "cactus", "island", "bridge", "lighthouse", "balloon", "kite", "snowman",
]

MAX_PLAYERS = 10
ROUND_SECONDS = 70
ROUNDS_PER_GAME = 3  # each player draws once per round


# ---------------- Data models ----------------
class Player:
    def __init__(self, pid: str, name: str, ws: WebSocket):
        self.id = pid
        self.name = name
        self.ws = ws
        self.score = 0
        self.connected = True

    def to_public(self, host_id: Optional[str] = None) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "score": self.score,
            "connected": self.connected,
            "is_host": self.id == host_id,
        }


class Room:
    def __init__(self, code: str):
        self.code = code
        self.players: Dict[str, Player] = {}
        self.host_id: Optional[str] = None
        self.state = "lobby"  # lobby | drawing | intermission | ended
        self.round = 0
        self.total_rounds = ROUNDS_PER_GAME
        self.drawer_order: List[str] = []
        self.drawer_index = 0
        self.current_word: Optional[str] = None
        self.word_choices: List[str] = []
        self.round_end_time: float = 0.0
        self.timer_task: Optional[asyncio.Task] = None
        self.correct_guessers: Set[str] = set()
        self.drawing_events: List[dict] = []

    # ---- player helpers ----
    def add_player(self, player: Player) -> bool:
        if len(self.players) >= MAX_PLAYERS:
            return False
        self.players[player.id] = player
        if self.host_id is None:
            self.host_id = player.id
        return True

    def remove_player(self, pid: str):
        if pid in self.players:
            del self.players[pid]
        if self.host_id == pid:
            self.host_id = next(iter(self.players), None)
        if pid in self.drawer_order:
            try:
                idx = self.drawer_order.index(pid)
                self.drawer_order.pop(idx)
                if idx < self.drawer_index:
                    self.drawer_index -= 1
            except ValueError:
                pass

    def public_players(self) -> List[dict]:
        return [p.to_public(self.host_id) for p in self.players.values()]

    def reset_for_new_game(self):
        self.round = 0
        self.state = "lobby"
        self.drawer_order = []
        self.drawer_index = 0
        self.current_word = None
        self.correct_guessers.clear()
        self.drawing_events.clear()
        for p in self.players.values():
            p.score = 0

    def broadcast_payload(self, msg_type: str, **kwargs) -> dict:
        return {"type": msg_type, "room": self.code, **kwargs}


# ---------------- Room manager ----------------
class RoomManager:
    def __init__(self):
        self.rooms: Dict[str, Room] = {}

    def create_room(self) -> Room:
        code = self._gen_code()
        room = Room(code)
        self.rooms[code] = room
        return room

    def _gen_code(self) -> str:
        while True:
            code = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
            if code not in self.rooms:
                return code

    def get_room(self, code: str) -> Optional[Room]:
        return self.rooms.get(code.upper())

    def cleanup_room(self, room: Room):
        if not any(p.connected for p in room.players.values()):
            if room.timer_task:
                room.timer_task.cancel()
            self.rooms.pop(room.code, None)


manager = RoomManager()


# ---------------- Helpers ----------------
async def send_json(ws: WebSocket, data: dict):
    try:
        await ws.send_text(json.dumps(data))
    except Exception:
        pass


async def broadcast(room: Room, data: dict, exclude: Optional[Set[str]] = None):
    exclude = exclude or set()
    dead: List[str] = []
    for pid, p in list(room.players.items()):
        if pid in exclude or not p.connected:
            continue
        try:
            await p.ws.send_text(json.dumps(data))
        except Exception:
            dead.append(pid)
    for pid in dead:
        if pid in room.players:
            room.players[pid].connected = False


def room_state_payload(room: Room) -> dict:
    return {
        "type": "room_state",
        "room": room.code,
        "state": room.state,
        "round": room.round,
        "total_rounds": room.total_rounds,
        "host_id": room.host_id,
        "players": room.public_players(),
        "drawer_id": room.drawer_order[room.drawer_index] if (
            0 <= room.drawer_index < len(room.drawer_order)
        ) else None,
        "word_length": len(room.current_word) if room.current_word else 0,
        "round_end_time": room.round_end_time,
    }


def masked_word(word: Optional[str]) -> str:
    if not word:
        return ""
    return " ".join("_" if c != " " else " " for c in word)


async def broadcast_room_state(room: Room):
    await broadcast(room, room_state_payload(room))


# ---------------- Round/timer logic ----------------
async def run_round_timer(room: Room):
    try:
        while room.state == "drawing":
            remaining = room.round_end_time - time.time()
            if remaining <= 0:
                break
            await asyncio.sleep(1)
        if room.state == "drawing":
            await end_round(room)
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        print(f"[timer] error in room {room.code}: {exc}")


async def start_round(room: Room):
    if room.timer_task:
        room.timer_task.cancel()
        room.timer_task = None

    if not room.drawer_order:
        room.state = "lobby"
        await broadcast_room_state(room)
        return

    if room.drawer_index >= len(room.drawer_order):
        room.round += 1
        if room.round >= room.total_rounds:
            room.state = "ended"
            await broadcast(room, {
                "type": "game_over",
                "players": room.public_players(),
            })
            await broadcast_room_state(room)
            return
        room.drawer_index = 0

    room.correct_guessers.clear()
    room.drawing_events.clear()
    room.current_word = random.choice(WORDS)
    room.round_end_time = time.time() + ROUND_SECONDS
    room.state = "drawing"

    drawer_id = room.drawer_order[room.drawer_index]

    # Tell drawer the word; others get a mask
    for pid, p in room.players.items():
        if not p.connected:
            continue
        if pid == drawer_id:
            await send_json(p.ws, {
                "type": "round_start",
                "word": room.current_word,
                "is_drawer": True,
                "drawer_id": drawer_id,
                "round": room.round,
                "total_rounds": room.total_rounds,
                "end_time": room.round_end_time,
                "duration": ROUND_SECONDS,
            })
        else:
            await send_json(p.ws, {
                "type": "round_start",
                "word": masked_word(room.current_word),
                "is_drawer": False,
                "drawer_id": drawer_id,
                "round": room.round,
                "total_rounds": room.total_rounds,
                "end_time": room.round_end_time,
                "duration": ROUND_SECONDS,
            })

    await broadcast_room_state(room)
    room.timer_task = asyncio.create_task(run_round_timer(room))


async def end_round(room: Room):
    if room.timer_task:
        room.timer_task.cancel()
        room.timer_task = None

    room.state = "intermission"
    await broadcast(room, {
        "type": "round_end",
        "word": room.current_word,
        "players": room.public_players(),
    })

    # advance drawer
    room.drawer_index += 1
    if room.drawer_index >= len(room.drawer_order):
        room.round += 1
        if room.round >= room.total_rounds:
            room.state = "ended"
            await broadcast(room, {
                "type": "game_over",
                "players": room.public_players(),
            })
            await broadcast_room_state(room)
            return
        room.drawer_index = 0

    await broadcast_room_state(room)

    # brief pause, then next round
    await asyncio.sleep(4)
    if room.state == "intermission":
        await start_round(room)


# ---------------- HTTP routes ----------------
@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/lobby", response_class=HTMLResponse)
async def lobby_page():
    return FileResponse(STATIC_DIR / "lobby.html")


@app.get("/game", response_class=HTMLResponse)
async def game_page():
    return FileResponse(STATIC_DIR / "game.html")


@app.get("/health")
async def health():
    return {"ok": True, "rooms": len(manager.rooms)}


# ---------------- WebSocket ----------------
@app.websocket("/ws/{room_code}/{player_id}")
async def ws_endpoint(ws: WebSocket, room_code: str, player_id: str):
    await ws.accept()
    room = manager.get_room(room_code)
    if not room:
        await send_json(ws, {"type": "error", "message": "Room not found"})
        await ws.close()
        return

    # Reconnect path
    existing = room.players.get(player_id)
    if existing:
        existing.ws = ws
        existing.connected = True
        if existing.id == room.host_id:
            pass  # already host
        await send_json(ws, {"type": "welcome_back", "player_id": player_id})
        await broadcast_room_state(room)
    else:
        # first message is join
        try:
            raw = await ws.receive_text()
            data = json.loads(raw)
        except Exception:
            await ws.close()
            return

        if data.get("type") != "join":
            await send_json(ws, {"type": "error", "message": "Expected join"})
            await ws.close()
            return

        name = (data.get("name") or "").strip()[:20] or "Player"
        player = Player(player_id, name, ws)
        if not room.add_player(player):
            await send_json(ws, {"type": "error", "message": "Room is full"})
            await ws.close()
            return

        await send_json(ws, {
            "type": "welcome",
            "player_id": player_id,
            "room": room.code,
        })
        await broadcast(room, {"type": "system", "message": f"{name} joined"})
        await broadcast_room_state(room)

    # Message loop
    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            await handle_message(room, player_id, msg)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        print(f"[ws] error: {exc}")
    finally:
        p = room.players.get(player_id)
        if p and p.ws is ws:
            p.connected = False
            await broadcast(room, {
                "type": "system",
                "message": f"{p.name} disconnected",
            })
            # remove if still disconnected and game in lobby
            await asyncio.sleep(0)
            await broadcast_room_state(room)
            asyncio.create_task(cleanup_later(room, player_id))


async def cleanup_later(room: Room, pid: str):
    await asyncio.sleep(30)
    p = room.players.get(pid)
    if p and not p.connected:
        room.remove_player(pid)
        # if host left, promote
        if room.host_id is None and room.players:
            room.host_id = next(iter(room.players))
        await broadcast_room_state(room)
        if room.state in ("drawing", "intermission") and pid in room.drawer_order:
            # drawer vanished mid-round; end current round
            await end_round(room)
    manager.cleanup_room(room)


# ---------------- Message handling ----------------
async def handle_message(room: Room, pid: str, msg: dict):
    mtype = msg.get("type")
    player = room.players.get(pid)
    if not player:
        return

    if mtype == "chat":
        text = (msg.get("text") or "").strip()[:200]
        if not text:
            return
        # guessing in drawing state
        if room.state == "drawing" and room.current_word and pid != (
            room.drawer_order[room.drawer_index] if room.drawer_index < len(room.drawer_order) else None
        ):
            if pid in room.correct_guessers:
                await broadcast(room, {
                    "type": "chat",
                    "player_id": pid,
                    "name": player.name,
                    "text": text,
                    "kind": "chat",
                })
                return
            guess = text.lower().strip()
            if guess == room.current_word.lower():
                # correct
                room.correct_guessers.add(pid)
                # points: faster = more, base 100
                time_left = max(0.0, room.round_end_time - time.time())
                pts = int(50 + 100 * (time_left / ROUND_SECONDS))
                player.score += pts
                drawer = room.players.get(
                    room.drawer_order[room.drawer_index]
                    if room.drawer_index < len(room.drawer_order) else ""
                )
                if drawer:
                    drawer.score += 25
                await broadcast(room, {
                    "type": "correct_guess",
                    "player_id": pid,
                    "name": player.name,
                    "points": pts,
                    "word": room.current_word,
                    "players": room.public_players(),
                })
                # If all guessers done, end early
                non_drawers = [
                    p for p_id, p in room.players.items()
                    if p_id != (room.drawer_order[room.drawer_index]
                                if room.drawer_index < len(room.drawer_order) else None)
                    and p.connected
                ]
                if non_drawers and all(p.id in room.correct_guessers for p in non_drawers):
                    await end_round(room)
                else:
                    await broadcast_room_state(room)
                return
            else:
                # close guess -> "so close"? keep simple: broadcast as chat
                await broadcast(room, {
                    "type": "chat",
                    "player_id": pid,
                    "name": player.name,
                    "text": text,
                    "kind": "chat",
                })
                return
        # lobby / intermission / ended chat
        await broadcast(room, {
            "type": "chat",
            "player_id": pid,
            "name": player.name,
            "text": text,
            "kind": "chat",
        })
        return

    if mtype == "draw":
        if room.state != "drawing":
            return
        drawer_id = room.drawer_order[room.drawer_index] if (
            0 <= room.drawer_index < len(room.drawer_order)
        ) else None
        if pid != drawer_id:
            return
        event = {
            "type": "draw",
            "x0": msg.get("x0"), "y0": msg.get("y0"),
            "x1": msg.get("x1"), "y1": msg.get("y1"),
            "color": msg.get("color", "#000000"),
            "size": msg.get("size", 4),
            "erase": bool(msg.get("erase", False)),
        }
        room.drawing_events.append(event)
        # cap memory
        if len(room.drawing_events) > 3000:
            room.drawing_events = room.drawing_events[-2000:]
        await broadcast(room, event, exclude={pid})
        return

    if mtype == "clear":
        if room.state != "drawing":
            return
        drawer_id = room.drawer_order[room.drawer_index] if (
            0 <= room.drawer_index < len(room.drawer_order)
        ) else None
        if pid != drawer_id:
            return
        room.drawing_events.clear()
        await broadcast(room, {"type": "clear"}, exclude={pid})
        return

    if mtype == "start_game":
        if pid != room.host_id:
            return
        if room.state != "lobby":
            return
        if len(room.players) < 2:
            await send_json(player.ws, {
                "type": "error",
                "message": "Need at least 2 players",
            })
            return
        room.reset_for_new_game()
        room.drawer_order = list(room.players.keys())
        random.shuffle(room.drawer_order)
        room.drawer_index = 0
        room.round = 0
        await broadcast(room, {"type": "game_starting"})
        await asyncio.sleep(0.5)
        await start_round(room)
        return

    if mtype == "play_again":
        if pid != room.host_id:
            return
        if room.state != "ended":
            return
        room.reset_for_new_game()
        await broadcast_room_state(room)
        return

    if mtype == "voice_signal":
        # WebRTC signaling relay
        target = msg.get("target")
        if target and target in room.players:
            await send_json(room.players[target].ws, {
                "type": "voice_signal",
                "from": pid,
                "signal": msg.get("signal"),
            })
        return
    if mtype == "voice_activity":
        await broadcast(room, {
            "type": "voice_activity",
            "player_id": pid,
            "speaking": bool(msg.get("speaking")),
        }, exclude={pid})
        return
    if mtype == "request_state":
        await send_json(player.ws, room_state_payload(room))
        return
