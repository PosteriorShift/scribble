"""
Scribble multiplayer — Skribbl-level.
FastAPI + WebSockets. In-memory. Render-ready.
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

try:
    import uvloop  # type: ignore
    uvloop.install()
except Exception:
    pass


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="Scribble")

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------- Config ----------------
# Grouped by difficulty so we can bias selection
WORDS: tuple[str, ...] = (
    # easy / short
    "cat", "dog", "sun", "moon", "star", "fish", "bird", "book", "chair",
    "table", "tree", "car", "bus", "hat", "cup", "key", "egg", "ball",
    "flower", "cloud", "rain", "snow", "boat", "train", "plane", "phone",
    "pizza", "cake", "coffee", "clock", "gift", "heart", "smile", "ghost",
    "robot", "dragon", "castle", "pirate", "ninja", "zombie", "rainbow",
    "volcano", "cactus", "island", "bridge", "balloon", "kite", "snowman",
    "apple", "banana", "burger", "guitar", "piano", "drum", "camera",
    "rocket", "mountain", "river", "penguin", "dolphin", "butterfly",
    "spider", "elephant", "giraffe", "lighthouse", "computer", "house",
)

MAX_PLAYERS = 10
ROUND_SECONDS = 70
INTERMISSION_SECONDS = 5
DISCONNECT_GRACE_SECONDS = 30
DRAW_BUFFER_MAX = 2000
WORD_CHOICES_PER_ROUND = 3
HINT_REVEAL_INTERVAL = 12   # seconds between revealed letters
MIN_PLAYERS = 2             # ← 2 players is the happy path now

ALLOWED_ROUND_COUNTS = (3, 5, 7)

_JSON_DUMPS = json.dumps


def jd(obj) -> str:
    return _JSON_DUMPS(obj, separators=(",", ":"), ensure_ascii=False)


# ---------------- Models ----------------
class Player:
    __slots__ = ("id", "name", "ws", "score", "connected", "is_host")

    def __init__(self, pid: str, name: str, ws: WebSocket):
        self.id = pid
        self.name = name
        self.ws = ws
        self.score = 0
        self.connected = True
        self.is_host = False

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
        "round_start_time", "timer_task", "cleanup_tasks", "correct_guessers",
        "drawing_events", "word_pool", "revealed_indices", "last_hint_time",
        "word_choices", "awaiting_choice",
    )

    def __init__(self, code: str):
        self.code = code
        self.players: Dict[str, Player] = {}
        self.host_id: Optional[str] = None
        self.state = "lobby"           # lobby | choosing | drawing | intermission | ended
        self.round = 0
        self.total_rounds = 3
        self.drawer_order: List[str] = []
        self.drawer_index = 0
        self.current_word: Optional[str] = None
        self.round_end_time: float = 0.0
        self.round_start_time: float = 0.0
        self.timer_task: Optional[asyncio.Task] = None
        self.cleanup_tasks: Set[asyncio.Task] = set()
        self.correct_guessers: Set[str] = set()
        self.drawing_events: deque = deque(maxlen=DRAW_BUFFER_MAX)
        self.word_pool: List[str] = []       # shuffled bag, no repeats
        self.revealed_indices: Set[int] = set()   # hint system
        self.last_hint_time: float = 0.0
        self.word_choices: List[str] = []
        self.awaiting_choice: Optional[str] = None   # drawer id who must pick

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

    def reset_for_new_game(self, rounds: int):
        self.round = 0
        self.total_rounds = rounds if rounds in ALLOWED_ROUND_COUNTS else 3
        self.state = "lobby"
        self.drawer_order = []
        self.drawer_index = 0
        self.current_word = None
        self.correct_guessers.clear()
        self.drawing_events.clear()
        self.word_pool = []
        self.revealed_indices.clear()
        self.word_choices = []
        self.awaiting_choice = None
        for p in self.players.values():
            p.score = 0

    def current_drawer_id(self) -> Optional[str]:
        if 0 <= self.drawer_index < len(self.drawer_order):
            return self.drawer_order[self.drawer_index]
        return None

    def next_word_choices(self, n: int = WORD_CHOICES_PER_ROUND) -> List[str]:
        """Pull n distinct words from the shuffled bag. Refill when empty."""
        if len(self.word_pool) < n:
            fresh = list(WORDS)
            random.shuffle(fresh)
            self.word_pool.extend(fresh)
        picks = []
        for _ in range(n):
            picks.append(self.word_pool.pop())
        return picks


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


# ---------------- Helpers ----------------
def mask_word(word: str, revealed: Set[int]) -> str:
    """Render 'c _ t' style with some letters revealed via hints."""
    out = []
    for i, ch in enumerate(word):
        if ch == " ":
            out.append(" ")
        elif i in revealed:
            out.append(ch)
        else:
            out.append("_")
    return " ".join(out)


def is_close_guess(guess: str, word: str) -> bool:
    """Levenshtein distance ≤ 1 → 'so close!'"""
    if guess == word:
        return False
    if abs(len(guess) - len(word)) > 1:
        return False
    # simple 1-edit distance check
    if len(guess) == len(word):
        diffs = sum(1 for a, b in zip(guess, word) if a != b)
        return diffs == 1
    # one insertion/deletion
    s, t = (guess, word) if len(guess) < len(word) else (word, guess)
    i = j = 0
    skipped = False
    while i < len(s) and j < len(t):
        if s[i] == t[j]:
            i += 1
            j += 1
        else:
            if skipped:
                return False
            skipped = True
            j += 1
    return True


async def send_json(ws: WebSocket, data: dict):
    try:
        await ws.send_text(jd(data))
    except Exception:
        pass


async def _try_send(p: Player, payload: str) -> Optional[str]:
    try:
        await p.ws.send_text(payload)
        return None
    except Exception:
        p.connected = False
        return p.id


async def broadcast(room: Room, data: dict, exclude: Optional[Set[str]] = None):
    payload = jd(data)
    tasks = []
    if exclude:
        for pid, p in room.players.items():
            if pid in exclude or not p.connected:
                continue
            tasks.append(asyncio.create_task(_try_send(p, payload)))
    else:
        for p in room.players.values():
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
        "round_start_time": room.round_start_time,
    }


async def broadcast_room_state(room: Room):
    await broadcast(room, room_state_payload(room))


# ---------------- Round / timer ----------------
async def run_round_timer(room: Room):
    """Precise sleep + hint revealer."""
    try:
        while room.state == "drawing":
            now = time.time()
            remaining = room.round_end_time - now
            if remaining <= 0:
                break
            # hint reveal based on elapsed time
            if room.current_word:
                elapsed = now - room.round_start_time
                target_reveals = int(elapsed / HINT_REVEAL_INTERVAL)
                # reveal this many letters (excluding first — Skribbl keeps first visible)
                if target_reveals > len(room.revealed_indices):
                    hidden = [i for i in range(len(room.current_word))
                              if i not in room.revealed_indices and room.current_word[i] != " "]
                    if hidden:
                        idx = random.choice(hidden)
                        room.revealed_indices.add(idx)
                        room.last_hint_time = now
                        await broadcast(room, {
                            "type": "hint",
                            "masked": mask_word(room.current_word, room.revealed_indices),
                        })
            await asyncio.sleep(min(1.0, remaining))
        if room.state == "drawing":
            await end_round(room)
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        print(f"[timer] room={room.code} err={exc}")


async def prompt_drawer_for_word(room: Room):
    """Ask the current drawer to pick from 3 choices."""
    drawer_id = room.current_drawer_id()
    drawer = room.players.get(drawer_id) if drawer_id else None
    if not drawer or not drawer.connected:
        # drawer gone → advance
        await start_round(room)
        return

    room.word_choices = room.next_word_choices(WORD_CHOICES_PER_ROUND)
    room.awaiting_choice = drawer_id
    room.state = "choosing"
    room.revealed_indices.clear()
    room.current_word = None

    await send_json(drawer.ws, {
        "type": "choose_word",
        "choices": room.word_choices,
        "round": room.round,
        "total_rounds": room.total_rounds,
        "end_time": time.time() + 15,   # 15s to pick or auto-pick
    })

    await broadcast(room, {
        "type": "waiting_for_word",
        "drawer_id": drawer_id,
        "drawer_name": drawer.name,
        "round": room.round,
        "total_rounds": room.total_rounds,
    }, exclude={drawer_id})

    await broadcast_room_state(room)

    # auto-pick after 15s if drawer is silent
    asyncio.create_task(auto_pick_word(room, drawer_id, room.word_choices[:]))


async def auto_pick_word(room: Room, drawer_id: str, choices: List[str]):
    await asyncio.sleep(15)
    if (room.state == "choosing"
            and room.awaiting_choice == drawer_id
            and room.word_choices == choices):
        # auto pick the first
        await begin_drawing(room, choices[0], drawer_id)


async def begin_drawing(room: Room, word: str, drawer_id: str):
    if room.awaiting_choice != drawer_id and room.current_word:
        return  # already started

    room.current_word = word
    room.awaiting_choice = None
    room.revealed_indices.clear()
    room.correct_guessers.clear()
    room.drawing_events.clear()
    room.round_start_time = time.time()
    room.round_end_time = room.round_start_time + ROUND_SECONDS
    room.state = "drawing"

    drawer_id = room.current_drawer_id()
    word = room.current_word
    masked = mask_word(word, room.revealed_indices)
    end_time = room.round_end_time
    start_time = room.round_start_time
    rnd = room.round
    trnd = room.total_rounds

    tasks = []
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
            "start_time": start_time,
            "duration": ROUND_SECONDS,
        }
        tasks.append(asyncio.create_task(send_json(p.ws, msg)))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    await broadcast_room_state(room)

    if room.timer_task and not room.timer_task.done():
        room.timer_task.cancel()
    room.timer_task = asyncio.create_task(run_round_timer(room))


async def start_round(room: Room):
    """Advance drawer_index and either prompt for word or end game."""
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

    # drawer may have disconnected — skip them
    guard = 0
    while room.drawer_index < len(room.drawer_order) and guard < len(room.drawer_order) + 1:
        did = room.drawer_order[room.drawer_index]
        p = room.players.get(did)
        if p and p.connected:
            break
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
        guard += 1

    await prompt_drawer_for_word(room)


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


# ---------------- HTTP ----------------
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
    room = manager.get_room(room_code)
    if not room:
        await send_json(ws, {"type": "error", "message": "Room not found"})
        await ws.close()
        return

    existing = room.players.get(player_id)
    if existing:
        old_ws = existing.ws
        existing.ws = ws
        existing.connected = True
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
        # if we're waiting for this player's word choice, re-send it
        if room.state == "choosing" and room.awaiting_choice == player_id:
            await send_json(ws, {
                "type": "choose_word",
                "choices": room.word_choices,
                "round": room.round,
                "total_rounds": room.total_rounds,
            })
    else:
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
        was_choosing = (room.awaiting_choice == pid)
        room.remove_player(pid)
        if room.host_id is None and room.players:
            room.host_id = next(iter(room.players))
        await broadcast_room_state(room)
        if (was_drawer or was_choosing) and room.state in ("drawing", "choosing", "intermission"):
            await end_round(room)
    manager.cleanup_room(room)


# ---------------- Message handling ----------------
async def handle_message(room: Room, pid: str, msg: dict):
    mtype = msg.get("type")
    player = room.players.get(pid)
    if not player:
        return

    # ---------- WORD CHOICE (drawer only) ----------
    if mtype == "pick_word":
        if room.state != "choosing" or room.awaiting_choice != pid:
            return
        word = (msg.get("word") or "").strip()
        if word not in room.word_choices:
            return
        await begin_drawing(room, word, pid)
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
            word_cf = room.current_word.casefold()

            if guess == word_cf:
                # ---- Correct ----
                room.correct_guessers.add(pid)
                time_left = max(0.0, room.round_end_time - time.time())
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

                # end early if all connected non-drawers guessed
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

            # ---- close guess ----
            if is_close_guess(guess, word_cf):
                await send_json(player.ws, {
                    "type": "close_guess",
                    "text": text,
                })
                return

        # ---- normal chat ----
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

    # ---------- SKIP (drawer gives up) ----------
    if mtype == "skip_word":
        if room.state != "drawing" or pid != room.current_drawer_id():
            return
        await broadcast(room, {
            "type": "system",
            "message": f"{player.name} skipped the word: {room.current_word}",
        })
        await end_round(room)
        return

    # ---------- SET ROUNDS (host, lobby only) ----------
    if mtype == "set_rounds":
        if pid != room.host_id or room.state != "lobby":
            return
        n = int(msg.get("rounds") or 3)
        if n not in ALLOWED_ROUND_COUNTS:
            n = 3
        room.total_rounds = n
        await broadcast_room_state(room)
        return

    # ---------- START GAME ----------
    if mtype == "start_game":
        if pid != room.host_id or room.state != "lobby":
            return
        if len(room.players) < MIN_PLAYERS:
            await send_json(player.ws, {
                "type": "error",
                "message": f"Need at least {MIN_PLAYERS} players",
            })
            return
        rounds = int(msg.get("rounds") or room.total_rounds or 3)
        room.reset_for_new_game(rounds)
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
        room.reset_for_new_game(room.total_rounds)
        await broadcast_room_state(room)
        return

    # ---------- VOICE ----------
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
