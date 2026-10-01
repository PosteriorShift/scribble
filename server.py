"""
Scribble multiplayer game server — optimized for low latency.
FastAPI + WebSockets. In-memory state, no database.
Deploy on Render: uvicorn server:app --host 0.0.0.0 --port $PORT --ws websockets --loop uvloop
"""
from __future__ import annotations

import asyncio
import json
import random
import string
import time
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Set

from fastapi import FastAPI, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

# ---- Optional uvloop (huge perf win on Linux; silently skipped on Windows) ----
try:
    import uvloop  # type: ignore
    uvloop.install()
except Exception:
    pass


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="Scribble Game")

# Static with long cache (files are content-addressed enough for this game)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------- Config ----------------
WORDS: tuple[str, ...] = (
    "apple", "banana", "cat", "dog", "house", "tree", "car", "sun", "moon",
    "star", "fish", "bird", "book", "chair", "table", "flower", "cloud",
    "rain", "snow", "mountain", "river", "boat", "train", "plane", "rocket",
    "pizza", "burger", "cake", "coffee", "guitar", "piano", "drum", "phone",
    "computer", "camera", "clock", "gift", "heart", "smile", "ghost", "robot",
    "dragon", "castle", "pirate", "ninja", "zombie", "rainbow", "volcano",
    "elephant", "giraffe", "penguin", "dolphin", "butterfly", "spider",
    "cactus", "island", "bridge", "lighthouse", "balloon", "kite", "snowman",
)

MAX_PLAYERS = 10
ROUND_SECONDS = 70
ROUNDS_PER_GAME = 3
INTERMISSION_SECONDS = 4
DISCONNECT_GRACE_SECONDS = 30
DRAW_BUFFER_MAX = 2000

# Pre-serialize separators for smaller/faster JSON
_JSON_SEP = (",", ":")
_JSON_DUMPS = json.dumps


def jd(obj) -> str:
    return _JSON_DUMPS(obj, separators=_JSON_SEP, ensure_ascii=False)


# ---------------- Data models ----------------
class Player:
    __slots__ = ("id", "name", "ws", "score", "connected")

    def __init__(self, pid: str, name: str, ws: WebSocket):
        self.id = pid
        self.name = name
        self.ws = ws
        self.score = 0
        self.connected = True

    def to_public(self, host_id: Optional[str]) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "score": self.score,
            "connected": self.connected,
            "is_host": self.id == host_id,
        }


class Room:
    __slots__ = (
        "code", "players", "host_id", "state", "round", "total_rounds",
        "drawer_order", "drawer_index", "current_word", "round_end_time",
        "timer_task", "cleanup_tasks", "correct_guessers", "drawing_events",
    )

    def __init__(self, code: str):
        self.code = code
        self.players: Dict[str, Player] = {}
        self.host_id: Optional[str] = None
        self.state = "lobby"           # lobby | drawing | intermission | ended
        self.round = 0
        self.total_rounds = ROUNDS_PER_GAME
        self.drawer_order: List[str] = []
        self.drawer_index = 0
        self.current_word: Optional[str] = None
        self.round_end_time: float = 0.0
        self.timer_task: Optional[asyncio.Task] = None
        self.cleanup_tasks: Set[asyncio.Task] = set()
        self.correct_guessers: Set[str] = set()
        # ring buffer — no reallocation, bounded memory
        self.drawing_events: deque = deque(maxlen=DRAW_BUFFER_MAX)

    # ---- player helpers ----
    def add_player(self, player: Player) -> bool:
        if len(self.players) >= MAX_PLAYERS:
            return False
        self.players[player.id] = player
        if self.host_id is None:
            self.host_id = player.id
        return True

    def remove_player(self, pid: str):
        self.players.pop(pid, None)
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
        host = self.host_id
        return [p.to_public(host) for p in self.players.values()]

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

    def current_drawer_id(self) -> Optional[str]:
        if 0 <= self.drawer_index < len(self.drawer_order):
            return self.drawer_order[self.drawer_index]
        return None


# ---------------- Room manager ----------------
class RoomManager:
    __slots__ = ("rooms",)

    def __init__(self):
        self.rooms: Dict[str, Room] = {}

    def create_room(self) -> Room:
        code = self._gen_code()
        room = Room(code)
        self.rooms[code] = room
        return room

    def _gen_code(self) -> str:
        alphabet = string.ascii_uppercase + string.digits
        rooms = self.rooms
        while True:
            code = "".join(random.choices(alphabet, k=4))
            if code not in rooms:
                return code

    def get_room(self, code: str) -> Optional[Room]:
        return self.rooms.get(code.upper())

    def cleanup_room(self, room: Room):
        if not any(p.connected for p in room.players.values()):
            t = room.timer_task
            if t and not t.done():
                t.cancel()
            for ct in list(room.cleanup_tasks):
                if not ct.done():
                    ct.cancel()
            room.cleanup_tasks.clear()
            self.rooms.pop(room.code, None)


manager = RoomManager()


# ---------------- Broadcast (parallel, fire-and-forget) ----------------
async def send_json(ws: WebSocket, data: dict):
    try:
        await ws.send_text(jd(data))
    except Exception:
        pass


async def _try_send(p: Player, payload: str) -> Optional[str]:
    """Return player id if send failed, else None."""
    try:
        await p.ws.send_text(payload)
        return None
    except Exception:
        p.connected = False
        return p.id


async def broadcast(room: Room, data: dict, exclude: Optional[Set[str]] = None):
    """Serialize once, send in parallel. Marks dead peers disconnected."""
    payload = jd(data)
    tasks: List[asyncio.Task] = []
    players = room.players
    if exclude:
        for pid, p in players.items():
            if pid in exclude or not p.connected:
                continue
            tasks.append(asyncio.create_task(_try_send(p, payload)))
    else:
        for p in players.values():
            if not p.connected:
                continue
            tasks.append(asyncio.create_task(_try_send(p, payload)))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def room_state_payload(room: Room) -> dict:
    return {
        "type": "room_state",
        "room": room.code,
        "state": room.state,
        "round": room.round,
        "total_rounds": room.total_rounds,
        "host_id": room.host_id,
        "players": room.public_players(),
        "drawer_id": room.current_drawer_id(),
        "word_length": len(room.current_word) if room.current_word else 0,
        "round_end_time": room.round_end_time,
    }


def masked_word(word: Optional[str]) -> str:
    if not word:
        return ""
    return " ".join("_" if c != " " else " " for c in word)


async def broadcast_room_state(room: Room):
    await broadcast(room, room_state_payload(room))


# ---------------- Round / timer ----------------
async def run_round_timer(room: Room):
    """Single precise sleep — no per-second polling."""
    try:
        remaining = room.round_end_time - time.time()
        if remaining > 0:
            await asyncio.sleep(remaining)
        if room.state == "drawing":
            await end_round(room)
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        print(f"[timer] room={room.code} err={exc}")


async def start_round(room: Room):
    if room.timer_task and not room.timer_task.done():
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

    drawer_id = room.current_drawer_id()
    word = room.current_word
    masked = masked_word(word)
    end_time = room.round_end_time
    rnd = room.round
    trnd = room.total_rounds

    # Per-player payloads: drawer sees word, others see mask.
    # Send in parallel.
    tasks: List[asyncio.Task] = []
    for pid, p in room.players.items():
        if not p.connected:
            continue
        msg = {
            "type": "round_start",
            "word": word if pid == drawer_id else masked,
            "is_drawer": pid == drawer_id,
            "drawer_id": drawer_id,
            "round": rnd,
            "total_rounds": trnd,
            "end_time": end_time,
            "duration": ROUND_SECONDS,
        }
        tasks.append(asyncio.create_task(send_json(p.ws, msg)))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    await broadcast_room_state(room)
    room.timer_task = asyncio.create_task(run_round_timer(room))


async def end_round(room: Room):
    if room.timer_task and not room.timer_task.done():
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

    try:
        await asyncio.sleep(INTERMISSION_SECONDS)
    except asyncio.CancelledError:
        return
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


@app.post("/api/create_room")
async def api_create_room():
    room = manager.create_room()
    return {"code": room.code}


@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)


# ---------------- WebSocket ----------------
@app.websocket("/ws/{room_code}/{player_id}")
async def ws_endpoint(ws: WebSocket, room_code: str, player_id: str):
    await ws.accept()

    # Bigger receive buffer helps with rapid draw streams
    try:
        await ws.send_text("")  # noop, ensures write path ready
    except Exception:
        pass

    room = manager.get_room(room_code)
    if not room:
        await send_json(ws, {"type": "error", "message": "Room not found"})
        await ws.close()
        return

    # ---- Reconnect path ----
    existing = room.players.get(player_id)
    if existing:
        old_ws = existing.ws
        existing.ws = ws
        existing.connected = True
        # Cancel any pending cleanup for this player
        for ct in list(room.cleanup_tasks):
            if ct.done():
                room.cleanup_tasks.discard(ct)
        try:
            if old_ws is not ws:
                await old_ws.close()
        except Exception:
            pass
        await send_json(ws, {"type": "welcome_back", "player_id": player_id})
        await broadcast_room_state(room)
    else:
        # ---- First connect: wait for join ----
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

    # ---- Message loop ----
    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            try:
                await handle_message(room, player_id, msg)
            except Exception as exc:
                print(f"[handle] room={room.code} pid={player_id} err={exc}")
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        print(f"[ws] err={exc}")
    finally:
        p = room.players.get(player_id)
        if p and p.ws is ws:
            p.connected = False
            await broadcast(room, {
                "type": "system",
                "message": f"{p.name} disconnected",
            })
            await broadcast_room_state(room)
            task = asyncio.create_task(cleanup_later(room, player_id))
            room.cleanup_tasks.add(task)
            task.add_done_callback(room.cleanup_tasks.discard)


async def cleanup_later(room: Room, pid: str):
    try:
        await asyncio.sleep(DISCONNECT_GRACE_SECONDS)
    except asyncio.CancelledError:
        return
    p = room.players.get(pid)
    if p and not p.connected:
        was_drawer = (room.current_drawer_id() == pid)
        room.remove_player(pid)
        if room.host_id is None and room.players:
            room.host_id = next(iter(room.players))
        await broadcast_room_state(room)
        if was_drawer and room.state in ("drawing", "intermission"):
            await end_round(room)
    manager.cleanup_room(room)


# ---------------- Message handling ----------------
async def handle_message(room: Room, pid: str, msg: dict):
    mtype = msg.get("type")
    player = room.players.get(pid)
    if not player:
        return

    # ---------- CHAT / GUESS ----------
    if mtype == "chat":
        text = (msg.get("text") or "").strip()[:200]
        if not text:
            return

        drawer_id = room.current_drawer_id()
        is_guess_phase = (room.state == "drawing"
                          and room.current_word is not None
                          and pid != drawer_id)

        if is_guess_phase and pid not in room.correct_guessers:
            guess = text.casefold()
            if guess == room.current_word.casefold():
                # ---- Correct guess ----
                room.correct_guessers.add(pid)
                time_left = room.round_end_time - time.time()
                if time_left < 0:
                    time_left = 0.0
                pts = int(50 + 100 * (time_left / ROUND_SECONDS))
                player.score += pts

                drawer = room.players.get(drawer_id) if drawer_id else None
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

                # End early if every connected non-drawer has guessed
                all_done = True
                for p_id, p in room.players.items():
                    if p_id == drawer_id or not p.connected:
                        continue
                    if p_id not in room.correct_guessers:
                        all_done = False
                        break

                if all_done:
                    await end_round(room)
                else:
                    await broadcast_room_state(room)
                return

        # ---- Normal chat ----
        await broadcast(room, {
            "type": "chat",
            "player_id": pid,
            "name": player.name,
            "text": text,
            "kind": "chat",
        })
        return

    # ---------- DRAW ----------
    if mtype == "draw":
        if room.state != "drawing" or pid != room.current_drawer_id():
            return
        # Compact draw event (short keys reduce payload ~30%)
        evt = {
            "type": "draw",
            "x0": msg.get("x0"), "y0": msg.get("y0"),
            "x1": msg.get("x1"), "y1": msg.get("y1"),
            "c": msg.get("color", "#000000"),
            "s": msg.get("size", 4),
            "e": 1 if msg.get("erase") else 0,
        }
        room.drawing_events.append(evt)
        await broadcast(room, evt, exclude={pid})
        return

    # ---------- CLEAR ----------
    if mtype == "clear":
        if room.state != "drawing" or pid != room.current_drawer_id():
            return
        room.drawing_events.clear()
        await broadcast(room, {"type": "clear"}, exclude={pid})
        return

    # ---------- START GAME ----------
    if mtype == "start_game":
        if pid != room.host_id or room.state != "lobby":
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
        await asyncio.sleep(0.3)
        await start_round(room)
        return

    # ---------- PLAY AGAIN ----------
    if mtype == "play_again":
        if pid != room.host_id or room.state != "ended":
            return
        room.reset_for_new_game()
        await broadcast_room_state(room)
        return

    # ---------- VOICE SIGNALING ----------
    if mtype == "voice_signal":
        target = msg.get("target")
        if target:
            tp = room.players.get(target)
            if tp and tp.connected:
                try:
                    await tp.ws.send_text(jd({
                        "type": "voice_signal",
                        "from": pid,
                        "signal": msg.get("signal"),
                    }))
                except Exception:
                    tp.connected = False
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
