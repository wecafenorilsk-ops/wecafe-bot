#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WeCafe Cleaning Bot (Telegram) — python-telegram-bot 21.x (polling)

What this version fixes (per your 3 issues + new requirements):
1) Photos from cleaning tasks are forwarded to the "Контроль" group (CONTROL_CHAT_ID) with caption.
   Also, if a user sends a photo when the bot isn't waiting for a photo, the bot will reply with guidance,
   and (optionally) still forward that photo to "Контроль" as "unlinked photo".
2) Re-registration loops: user/slot/cleaning data are stored in DATA_DIR.
   - On Render, attach a Persistent Disk mounted at /var/data, and set DATA_DIR=/var/data in Environment.
3) Auto-registration window 09:00–15:00:
   - Inside window: user is auto-activated (limited locks).
   - Outside window: user becomes "pending" and an approval request with buttons is sent to "Контроль".

Other included behavior:
- Tasks are "for the whole day + point", but responsibility is split into 2 zones A/B.
  First half-shift slot gets A, second gets B; whole shift gets ALL tasks.
- Reminders every 30 minutes:
  - First reminder for "cleaning not done" is a strict "косяк" message once.
  - Next reminders are gentle (no repeating the same "косяк" text).
- End-of-day financial report only after closing time:
    69 Параллель, Арена: after 22:00
    Кафе Музей: after 19:00
- Tiny HTTP health server for Render Web Service port scan (returns "ok").
- Suppresses httpx/httpcore INFO logs to avoid printing BOT_TOKEN in logs.

ENV (Render -> Environment):
BOT_TOKEN=...
ADMIN_IDS=123,456
CONTROL_CHAT_ID=-100...
ACCESS_CODE=wecafe2026
SCHEDULE_CSV_URL=https://docs.google.com/spreadsheets/d/<ID>/export?format=csv&gid=<GID>
TZ=Asia/Krasnoyarsk   # Norilsk
AUTO_APPROVE_START=09:00
AUTO_APPROVE_END=15:00
REMINDER_INTERVAL_MIN=30
DATA_DIR=/var/data     # IMPORTANT on Render with Persistent Disk
PORT=10000             # Render provides PORT automatically
"""

import csv
import json
import logging
import os
import re
import threading
from datetime import datetime, time, timedelta, date
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import StringIO
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
    Message,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# ----------------- ENV -----------------

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is empty. Set it in Render -> Environment.")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
CONTROL_CHAT_ID = int(os.getenv("CONTROL_CHAT_ID", "0").strip() or "0")

ACCESS_CODE = os.getenv("ACCESS_CODE", "wecafe2026").strip()

TZ_NAME = os.getenv("TZ", "Asia/Krasnoyarsk").strip()  # Norilsk
TZ = ZoneInfo(TZ_NAME)

SCHEDULE_CSV_URL = os.getenv("SCHEDULE_CSV_URL", "").strip()

AUTO_APPROVE_START = os.getenv("AUTO_APPROVE_START", "09:00").strip()
AUTO_APPROVE_END = os.getenv("AUTO_APPROVE_END", "15:00").strip()

REMINDER_INTERVAL_MIN = int(os.getenv("REMINDER_INTERVAL_MIN", "30").strip() or "30")

PORT = int(os.getenv("PORT", "10000").strip() or "10000")

DATA_DIR = (os.getenv("DATA_DIR") or "").strip()
if not DATA_DIR:
    # Prefer /var/data if it exists (Render persistent disk mount point)
    DATA_DIR = "/var/data" if os.path.isdir("/var/data") else os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

POINTS = ["69 Параллель", "Арена", "Кафе Музей"]

POINT_HOURS = {
    "69 Параллель": (time(10, 0), time(22, 0)),
    "Арена": (time(10, 0), time(22, 0)),
    "Кафе Музей": (time(9, 0), time(19, 0)),
}

# ----------------- LOGGING -----------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# --- Security: hide Telegram token from logs (httpx prints request URLs) ---
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

log = logging.getLogger("wecafe-bot")

# ----------------- FILE PATHS -----------------

USERS_PATH = os.path.join(DATA_DIR, "users.json")
SLOTS_PATH = os.path.join(DATA_DIR, "slots.json")
CLEANING_PATH = os.path.join(DATA_DIR, "cleaning.json")
VIOLATIONS_PATH = os.path.join(DATA_DIR, "violations.json")
REPORTS_PATH = os.path.join(DATA_DIR, "reports.json")


def _load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        log.error("Failed to load %s: %s", path, e)
        return default


def _save_json(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_users() -> Dict[str, dict]:
    return _load_json(USERS_PATH, {})


def save_users(users: Dict[str, dict]) -> None:
    _save_json(USERS_PATH, users)


def load_slots() -> Dict[str, dict]:
    return _load_json(SLOTS_PATH, {})


def save_slots(slots: Dict[str, dict]) -> None:
    _save_json(SLOTS_PATH, slots)


def load_cleaning() -> Dict[str, dict]:
    return _load_json(CLEANING_PATH, {})


def save_cleaning(cleaning: Dict[str, dict]) -> None:
    _save_json(CLEANING_PATH, cleaning)


def load_violations() -> Dict[str, dict]:
    return _load_json(VIOLATIONS_PATH, {"sent": {}, "gentle_sent": {}})


def save_violations(v: Dict[str, dict]) -> None:
    _save_json(VIOLATIONS_PATH, v)


def load_reports() -> Dict[str, dict]:
    return _load_json(REPORTS_PATH, {})


def save_reports(r: Dict[str, dict]) -> None:
    _save_json(REPORTS_PATH, r)


# ----------------- HEALTH SERVER (Render) -----------------

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def start_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), _HealthHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        log.info("Health server listening on port %s", PORT)
    except Exception as e:
        log.error("Health server failed: %s", e)


# ----------------- HELPERS -----------------

def now_tz() -> datetime:
    return datetime.now(TZ)


def today_key() -> str:
    return now_tz().date().isoformat()


def day_point_key(day: str, point: str) -> str:
    return f"{day}::{point}"


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def parse_hhmm(s: str) -> time:
    m = re.match(r"^\s*(\d{1,2})\s*:\s*(\d{2})\s*$", s)
    if not m:
        raise ValueError("bad time")
    h = int(m.group(1))
    mi = int(m.group(2))
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        raise ValueError("bad time")
    return time(h, mi)


def in_auto_approve_window(dt: datetime) -> bool:
    try:
        start = parse_hhmm(AUTO_APPROVE_START)
        end = parse_hhmm(AUTO_APPROVE_END)
    except Exception:
        return True
    st = dt.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
    en = dt.replace(hour=end.hour, minute=end.minute, second=0, microsecond=0)
    return st <= dt <= en


def point_open_time(point: str) -> time:
    return POINT_HOURS.get(point, (time(10, 0), time(22, 0)))[0]


def point_close_time(point: str) -> time:
    return POINT_HOURS.get(point, (time(10, 0), time(22, 0)))[1]


def after_close_now(point: str) -> bool:
    ct = point_close_time(point)
    dt = now_tz()
    close_dt = dt.replace(hour=ct.hour, minute=ct.minute, second=0, microsecond=0)
    return dt >= close_dt


def slot_duration_minutes(start: time, end: time) -> int:
    d = now_tz().date()
    s = datetime(d.year, d.month, d.day, start.hour, start.minute, tzinfo=TZ)
    e = datetime(d.year, d.month, d.day, end.hour, end.minute, tzinfo=TZ)
    if e < s:
        e += timedelta(days=1)
    return int((e - s).total_seconds() // 60)


def shift_duration_minutes(point: str) -> int:
    return slot_duration_minutes(point_open_time(point), point_close_time(point))


def is_within_point_hours(point: str) -> bool:
    dt = now_tz()
    op = point_open_time(point)
    cl = point_close_time(point)
    op_dt = dt.replace(hour=op.hour, minute=op.minute, second=0, microsecond=0)
    cl_dt = dt.replace(hour=cl.hour, minute=cl.minute, second=0, microsecond=0)
    return op_dt <= dt <= cl_dt


# ----------------- USERS -----------------

def user_name(users: Dict[str, dict], user_id: int) -> str:
    u = users.get(str(user_id))
    return (u or {}).get("name") or f"user{user_id}"


def is_active_user(u: Optional[dict]) -> bool:
    return bool(u and u.get("active", False))


def is_limited_user(u: Optional[dict]) -> bool:
    return bool(u and u.get("limited", False))


def is_pending_user(u: Optional[dict]) -> bool:
    return bool(u and u.get("pending", False))


def can_use_feature(u: Optional[dict], feature: str) -> bool:
    """
    feature:
      - choose_point, slot, plan, mark, transfer, incident, report, admin
    """
    if not u:
        return False
    if is_admin(u.get("id", 0)):
        return True
    if not is_active_user(u):
        return False
    if is_limited_user(u):
        return feature in {"choose_point", "slot", "plan", "mark"}
    return True


# ----------------- SCHEDULE LOADING -----------------

def fetch_schedule_csv() -> str:
    if not SCHEDULE_CSV_URL:
        raise RuntimeError("SCHEDULE_CSV_URL is empty")
    resp = requests.get(SCHEDULE_CSV_URL, timeout=25)
    resp.raise_for_status()
    return resp.text


def fix_mojibake(s: str) -> str:
    # Fix "Ð¨ÐºÐ°ÑÑ" style text if it happens
    if "Ð" in s or "Ñ" in s:
        try:
            return s.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            return s
    return s


def parse_schedule_tasks_for_today(schedule_csv_text: str, day: date) -> List[str]:
    """
    Supports 2 common CSV shapes:

    (A) "list" CSV with columns: task (or Задача) and optional day/date column.
        day can be YYYY-MM-DD or day-of-month number.

    (B) "matrix" CSV:
        first row: [task_name_col, 1,2,3,...,31]
        next rows: [task_name, x/1/yes, ...]
        A non-empty cell under today's day means the task is scheduled for today.

    If you have separate sheets per point, set SCHEDULE_CSV_URL accordingly per point.
    """
    txt = schedule_csv_text
    txt = fix_mojibake(txt)

    # Try dict mode first
    f = StringIO(txt)
    reader = csv.DictReader(f)
    if reader.fieldnames and any((fn or "").strip().lower() in {"task", "задача", "задачи"} for fn in reader.fieldnames):
        out = []
        day_iso = day.isoformat()
        day_num = str(day.day)
        for row in reader:
            r_task = (row.get("task") or row.get("задача") or row.get("Задача") or "").strip()
            if not r_task:
                continue
            r_day = (row.get("day") or row.get("date") or row.get("день") or row.get("Дата") or "").strip()
            if r_day and (r_day != day_iso and r_day != day_num):
                continue
            out.append(fix_mojibake(r_task))
        if out:
            return out

    # Matrix mode
    f2 = StringIO(txt)
    rows = list(csv.reader(f2))
    if not rows or len(rows) < 2:
        return []

    header = [c.strip() for c in rows[0]]
    # Find column for today's day number
    day_col = None
    for i, h in enumerate(header):
        if h.strip() == str(day.day):
            day_col = i
            break
    if day_col is None:
        # some exports use "01", "02"
        dd = f"{day.day:02d}"
        for i, h in enumerate(header):
            if h.strip() == dd:
                day_col = i
                break
    if day_col is None:
        return []

    tasks = []
    for r in rows[1:]:
        if not r:
            continue
        task_name = (r[0] if len(r) > 0 else "").strip()
        if not task_name:
            continue
        cell = (r[day_col] if len(r) > day_col else "").strip()
        if cell:
            tasks.append(fix_mojibake(task_name))
    return tasks


def split_tasks_two_zones(tasks: List[str]) -> Tuple[List[str], List[str]]:
    a, b = [], []
    for i, t in enumerate(tasks):
        (a if i % 2 == 0 else b).append(t)
    return a, b


# ----------------- CLEANING STATE -----------------

def ensure_tasks_loaded(day: str, point: str) -> List[str]:
    cleaning = load_cleaning()
    k = day_point_key(day, point)
    state = cleaning.get(k) or {"day": day, "point": point, "tasks": [], "split": {}, "done": {}}

    if state.get("tasks"):
        return state["tasks"]

    # Load from schedule
    if not SCHEDULE_CSV_URL:
        tasks = [
            "Шкафы: прибраться внутри",
            "Проверка на ротацию продукции",
            "Дверцы шкафчиков и стойка со стороны гостей",
            "Кофемашину сверху протереть",
        ]
    else:
        txt = fetch_schedule_csv()
        tasks = parse_schedule_tasks_for_today(txt, datetime.fromisoformat(day).date())

    # Store split map
    split = {str(i): ("A" if i % 2 == 0 else "B") for i in range(len(tasks))}
    state["tasks"] = tasks
    state["split"] = split
    cleaning[k] = state
    save_cleaning(cleaning)
    return tasks


def tasks_for_group(day: str, point: str, group: str) -> List[Tuple[int, str]]:
    tasks = ensure_tasks_loaded(day, point)
    cleaning = load_cleaning()
    state = cleaning[day_point_key(day, point)]
    out = []
    for idx, t in enumerate(tasks):
        g = state["split"].get(str(idx), "A")
        if group == "ALL" or g == group:
            out.append((idx, t))
    return out


def remaining_task_indices(day: str, point: str, group: str) -> List[int]:
    cleaning = load_cleaning()
    state = cleaning.get(day_point_key(day, point))
    if not state or not state.get("tasks"):
        ensure_tasks_loaded(day, point)
        cleaning = load_cleaning()
        state = cleaning.get(day_point_key(day, point), {"done": {}, "split": {}, "tasks": []})

    done = state.get("done", {})
    remaining = []
    for idx, _ in tasks_for_group(day, point, group):
        if str(idx) not in done:
            remaining.append(idx)
    return remaining


def mark_task_done(day: str, point: str, idx: int, by_user_id: int, by_name: str, photo_file_id: str):
    cleaning = load_cleaning()
    k = day_point_key(day, point)
    state = cleaning.get(k)
    if not state:
        ensure_tasks_loaded(day, point)
        cleaning = load_cleaning()
        state = cleaning.get(k)

    state.setdefault("done", {})
    state["done"][str(idx)] = {
        "by": by_user_id,
        "by_name": by_name,
        "ts": now_tz().isoformat(),
        "photo": photo_file_id,
    }
    cleaning[k] = state
    save_cleaning(cleaning)


# ----------------- SLOT STATE (A/B responsibility) -----------------

def new_slot_id() -> str:
    return f"s{int(now_tz().timestamp())}{os.getpid()}"


def open_slot_for_user(user_id: int) -> Optional[dict]:
    slots = load_slots()
    for s in slots.values():
        if s.get("user_id") == user_id and s.get("status") == "open":
            return s
    return None


def slots_for_day_point(day: str, point: str) -> List[dict]:
    slots = load_slots()
    out = [s for s in slots.values() if s.get("day") == day and s.get("point") == point]
    out.sort(key=lambda x: x.get("opened_at", ""))
    return out


def decide_group_for_slot(day: str, point: str, full_shift: bool) -> str:
    if full_shift:
        return "ALL"
    slots = slots_for_day_point(day, point)
    count = len(slots)
    # first => A, second => B, others => B
    if count == 0:
        return "A"
    if count == 1:
        return "B"
    return "B"


def slot_is_full_shift(point: str, start: time, end: time) -> bool:
    dur = slot_duration_minutes(start, end)
    full = shift_duration_minutes(point)
    return dur >= int(full * 0.85)


def get_user_group_for_today(uid: int, day: str, point: str) -> str:
    # prefer open slot; else last slot for today
    slots = slots_for_day_point(day, point)
    user_slots = [s for s in slots if s.get("user_id") == uid]
    if not user_slots:
        return "A"
    # open first
    for s in user_slots:
        if s.get("status") == "open":
            return s.get("group", "A")
    return user_slots[-1].get("group", "A")


# ----------------- VIOLATIONS (no spam) -----------------

def vkey(slot_id: str, vtype: str) -> str:
    return f"{slot_id}::{vtype}"


def v_already_sent(slot_id: str, vtype: str) -> bool:
    v = load_violations()
    return bool(v.get("sent", {}).get(vkey(slot_id, vtype)))


def v_already_gentle(slot_id: str, vtype: str) -> bool:
    v = load_violations()
    return bool(v.get("gentle_sent", {}).get(vkey(slot_id, vtype)))


def v_set_sent(slot_id: str, vtype: str, strict: bool, value: bool = True):
    v = load_violations()
    if strict:
        store = v.setdefault("sent", {})
    else:
        store = v.setdefault("gentle_sent", {})
    key = vkey(slot_id, vtype)
    if value:
        store[key] = now_tz().isoformat()
    else:
        store.pop(key, None)
    save_violations(v)


def v_clear_for_user(uid: int, vtype: str):
    slot = open_slot_for_user(uid)
    if not slot:
        return
    v_set_sent(slot["id"], vtype, strict=True, value=False)
    v_set_sent(slot["id"], vtype, strict=False, value=False)


# ----------------- UI -----------------

def main_menu_kb(u: dict) -> InlineKeyboardMarkup:
    btns = [
        [InlineKeyboardButton("🏷 Выбрать точку", callback_data="choose_point")],
        [InlineKeyboardButton("▶️ Открыть слот (начать работу)", callback_data="open_slot")],
        [InlineKeyboardButton("🧹 План уборки сегодня", callback_data="plan_today")],
        [InlineKeyboardButton("✅ Отметить выполненное (с фото)", callback_data="mark_tasks")],
    ]
    if not is_limited_user(u):
        btns += [
            [InlineKeyboardButton("🔁 Передать точку (закрыть слот)", callback_data="transfer_point")],
            [InlineKeyboardButton("🧾 Фин. отчёт (закрытие дня)", callback_data="fin_report")],
            [InlineKeyboardButton("💬 Инцидент / Комментарий", callback_data="incident")],
        ]
    if is_admin(u.get("id", 0)):
        btns.append([InlineKeyboardButton("🛠 Админка", callback_data="admin")])
    return InlineKeyboardMarkup(btns)


def points_kb(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(p, callback_data=f"{prefix}:{p}")] for p in POINTS])


# ----------------- CONVERSATION STATES -----------------

REG_NAME, REG_CODE = range(2)

SLOT_POINT, SLOT_MODE, SLOT_TIME = range(3)

MARK_PICK_TASK, MARK_WAIT_PHOTO = range(2)

TRANSFER_WAIT_COMMENT = 1

REPORT_CASH, REPORT_CASHLESS, REPORT_PHOTO = range(3)

INCIDENT_WAIT = 1


# ----------------- START / REGISTER -----------------

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    users = load_users()
    uid = update.effective_user.id
    u = users.get(str(uid))

    if u and is_active_user(u):
        await update.message.reply_text("Меню:", reply_markup=main_menu_kb(u))
        return ConversationHandler.END

    if u and is_pending_user(u):
        await update.message.reply_text(
            "⏳ Твоя регистрация ожидает одобрения руководителем.\n"
            "Напиши позже или дождись подтверждения.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    await update.message.reply_text("Привет! Напиши своё имя (как тебя записать).", reply_markup=ReplyKeyboardRemove())
    return REG_NAME


async def reg_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = (update.message.text or "").strip()
    if len(name) < 2:
        await update.message.reply_text("Напиши имя нормально (минимум 2 буквы).")
        return REG_NAME

    context.user_data["reg_name"] = name
    await update.message.reply_text("Теперь введи код доступа.")
    return REG_CODE


async def reg_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = (update.message.text or "").strip()
    uid = update.effective_user.id
    name = context.user_data.get("reg_name") or (update.effective_user.first_name or "Сотрудник")

    if code != ACCESS_CODE:
        await update.message.reply_text("Код неверный. Попробуй ещё раз.")
        return REG_CODE

    dt = now_tz()
    auto = in_auto_approve_window(dt)

    users = load_users()

    if auto:
        users[str(uid)] = {
            "id": uid,
            "name": name,
            "active": True,
            "pending": False,
            "limited": True,  # locks
            "registered_at": dt.isoformat(),
            "auto_approved": True,
        }
        save_users(users)

        if CONTROL_CHAT_ID:
            try:
                await context.bot.send_message(
                    chat_id=CONTROL_CHAT_ID,
                    text=f"✅ Автоактивация: {name} ({uid}) [09:00–15:00, ограниченный доступ]"
                )
            except Exception:
                pass

        await update.message.reply_text(
            "✅ Ок! Ты активирован (ограниченный доступ).\n"
            "Доступно: выбрать точку, открыть слот, смотреть план и отмечать задачи с фото.\n\nМеню:",
            reply_markup=main_menu_kb(users[str(uid)])
        )
        return ConversationHandler.END

    # Outside auto window -> pending approval
    users[str(uid)] = {
        "id": uid,
        "name": name,
        "active": False,
        "pending": True,
        "limited": True,
        "registered_at": dt.isoformat(),
        "auto_approved": False,
    }
    save_users(users)

    await update.message.reply_text(
        "⏳ Заявка на регистрацию отправлена руководителю.\n"
        "Как только одобрят — бот напишет тебе.",
        reply_markup=ReplyKeyboardRemove()
    )

    if CONTROL_CHAT_ID:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Одобрить", callback_data=f"approve_user:{uid}"),
            InlineKeyboardButton("❌ Отказать", callback_data=f"deny_user:{uid}"),
        ]])
        try:
            await context.bot.send_message(
                chat_id=CONTROL_CHAT_ID,
                text=f"🟡 Заявка на регистрацию:\nИмя: {name}\nuser_id: {uid}\n(вне окна 09:00–15:00)",
                reply_markup=kb
            )
        except Exception:
            pass

    return ConversationHandler.END


async def approve_user_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    admin_id = update.effective_user.id
    if not is_admin(admin_id):
        await q.edit_message_text("Нет прав.")
        return
    uid = int(q.data.split(":", 1)[1])

    users = load_users()
    u = users.get(str(uid))
    if not u:
        await q.edit_message_text("Пользователь не найден.")
        return
    u["active"] = True
    u["pending"] = False
    u["limited"] = True
    u["approved_at"] = now_tz().isoformat()
    u["approved_by"] = admin_id
    users[str(uid)] = u
    save_users(users)

    try:
        await context.bot.send_message(chat_id=uid, text="✅ Тебя одобрили. Напиши /start")
    except Exception:
        pass

    await q.edit_message_text(f"✅ Одобрено: {u.get('name')} ({uid})")


async def deny_user_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    admin_id = update.effective_user.id
    if not is_admin(admin_id):
        await q.edit_message_text("Нет прав.")
        return
    uid = int(q.data.split(":", 1)[1])

    users = load_users()
    u = users.get(str(uid))
    if not u:
        await q.edit_message_text("Пользователь не найден.")
        return

    # keep record but disable
    u["active"] = False
    u["pending"] = False
    u["denied_at"] = now_tz().isoformat()
    u["denied_by"] = admin_id
    users[str(uid)] = u
    save_users(users)

    try:
        await context.bot.send_message(chat_id=uid, text="❌ Регистрация не одобрена. Обратись к руководителю.")
    except Exception:
        pass

    await q.edit_message_text(f"❌ Отказано: {u.get('name')} ({uid})")


# ----------------- CHOOSE POINT -----------------

async def choose_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    users = load_users()
    u = users.get(str(update.effective_user.id))
    if not u:
        await q.edit_message_text("Нужно /start")
        return
    if not can_use_feature(u, "choose_point"):
        await q.edit_message_text("Нет доступа.")
        return
    await q.edit_message_text("Выбери точку:", reply_markup=points_kb("set_point"))


async def set_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    point = q.data.split(":", 1)[1]
    users = load_users()
    uid = update.effective_user.id
    u = users.get(str(uid))
    if not u:
        await q.edit_message_text("Нужно /start")
        return

    u["point"] = point
    users[str(uid)] = u
    save_users(users)

    await q.edit_message_text(f"✅ Точка установлена: <b>{html_escape(point)}</b>", parse_mode=ParseMode.HTML)
    await q.message.reply_text("Меню:", reply_markup=main_menu_kb(u))


# ----------------- OPEN SLOT -----------------

async def open_slot_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    users = load_users()
    u = users.get(str(update.effective_user.id))
    if not u:
        await q.edit_message_text("Нужно /start")
        return ConversationHandler.END
    if not can_use_feature(u, "slot"):
        await q.edit_message_text("Нет доступа.")
        return ConversationHandler.END

    if open_slot_for_user(update.effective_user.id):
        await q.edit_message_text("У тебя уже есть открытый слот. Закрой его через 'Передать точку'.", reply_markup=main_menu_kb(u))
        return ConversationHandler.END

    # choose point (default to user's point)
    await q.edit_message_text("Для какого места открываем слот? (можешь выбрать заново)", reply_markup=points_kb("slot_point"))
    return SLOT_POINT


async def slot_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    point = q.data.split(":", 1)[1]
    context.user_data["slot_point"] = point

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🟩 Смена целиком", callback_data="slot_mode:full")],
        [InlineKeyboardButton("🟨 Со скольки до скольки", callback_data="slot_mode:range")],
    ])
    await q.edit_message_text(f"Точка: <b>{html_escape(point)}</b>\nВыбери режим:", parse_mode=ParseMode.HTML, reply_markup=kb)
    return SLOT_MODE


async def slot_mode_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    mode = q.data.split(":", 1)[1]
    point = context.user_data.get("slot_point")
    if not point:
        await q.edit_message_text("Сначала выбери точку.")
        return ConversationHandler.END

    if mode == "full":
        op = point_open_time(point)
        cl = point_close_time(point)
        await create_slot(update, context, point, op, cl)
        users = load_users()
        u = users.get(str(update.effective_user.id), {})
        await q.edit_message_text("✅ Слот открыт (смена целиком).", reply_markup=main_menu_kb(u))
        return ConversationHandler.END

    await q.edit_message_text("Напиши время так: <b>HH:MM-HH:MM</b>\nНапример: <b>10:00-16:00</b>", parse_mode=ParseMode.HTML)
    return SLOT_TIME


async def slot_time_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    m = re.match(r"^\s*(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})\s*$", text)
    if not m:
        await update.message.reply_text("Нужно так: HH:MM-HH:MM. Например 10:00-16:00")
        return SLOT_TIME
    start = parse_hhmm(m.group(1))
    end = parse_hhmm(m.group(2))
    point = context.user_data.get("slot_point")
    if not point:
        await update.message.reply_text("Сначала выбери точку.")
        return ConversationHandler.END

    await create_slot(update, context, point, start, end)
    users = load_users()
    u = users.get(str(update.effective_user.id), {})
    await update.message.reply_text("✅ Слот открыт.", reply_markup=main_menu_kb(u))
    return ConversationHandler.END


async def create_slot(update: Update, context: ContextTypes.DEFAULT_TYPE, point: str, start: time, end: time):
    uid = update.effective_user.id
    day = today_key()
    full = slot_is_full_shift(point, start, end)
    group = decide_group_for_slot(day, point, full)

    slot = {
        "id": new_slot_id(),
        "user_id": uid,
        "day": day,
        "point": point,
        "start": start.strftime("%H:%M"),
        "end": end.strftime("%H:%M"),
        "group": group,
        "status": "open",
        "opened_at": now_tz().isoformat(),
    }
    slots = load_slots()
    slots[slot["id"]] = slot
    save_slots(slots)

    # also store user's point as current
    users = load_users()
    u = users.get(str(uid))
    if u:
        u["point"] = point
        users[str(uid)] = u
        save_users(users)

    if CONTROL_CHAT_ID:
        try:
            users = load_users()
            name = user_name(users, uid)
            await context.bot.send_message(
                chat_id=CONTROL_CHAT_ID,
                text=f"▶️ Слот открыт: {point} | {name} ({uid}) | {slot['start']}-{slot['end']} | зона {group}",
            )
        except Exception:
            pass


# ----------------- PLAN TODAY -----------------

async def plan_today_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    users = load_users()
    u = users.get(str(update.effective_user.id))
    if not u:
        await q.edit_message_text("Нужно /start")
        return
    if not can_use_feature(u, "plan"):
        await q.edit_message_text("Нет доступа.")
        return

    point = u.get("point")
    if not point:
        await q.edit_message_text("Сначала выбери точку.", reply_markup=points_kb("set_point"))
        return

    day = today_key()
    group = get_user_group_for_today(update.effective_user.id, day, point)

    tasks = tasks_for_group(day, point, group)
    state = load_cleaning().get(day_point_key(day, point), {})
    done = state.get("done", {})

    lines = [f"<b>План на сегодня ({html_escape(point)})</b>", f"Зона ответственности: <b>{group}</b>"]
    for idx, t in tasks:
        mark = "✅" if str(idx) in done else "⬜️"
        lines.append(f"{mark} {html_escape(t)}")

    rem = remaining_task_indices(day, point, group)
    lines.append("")
    lines.append(f"Осталось: <b>{len(rem)}</b>")

    await q.edit_message_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(u))


# ----------------- MARK TASKS (with photo -> forward to CONTROL) -----------------

async def mark_tasks_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    users = load_users()
    u = users.get(str(update.effective_user.id))
    if not u:
        await q.edit_message_text("Нужно /start")
        return ConversationHandler.END
    if not can_use_feature(u, "mark"):
        await q.edit_message_text("Нет доступа.")
        return ConversationHandler.END

    point = u.get("point")
    if not point:
        await q.edit_message_text("Сначала выбери точку.", reply_markup=points_kb("set_point"))
        return ConversationHandler.END

    day = today_key()
    group = get_user_group_for_today(update.effective_user.id, day, point)
    rem = remaining_task_indices(day, point, group)

    if not rem:
        await q.edit_message_text("✅ Всё по твоей зоне закрыто. Отлично.", reply_markup=main_menu_kb(u))
        v_clear_for_user(update.effective_user.id, "cleaning_pending")
        return ConversationHandler.END

    state = load_cleaning().get(day_point_key(day, point), {})
    tasks = state.get("tasks") or ensure_tasks_loaded(day, point)

    buttons = []
    for idx in rem[:40]:
        title = tasks[idx]
        buttons.append([InlineKeyboardButton(title[:50], callback_data=f"do_task:{idx}")])

    await q.edit_message_text(
        f"Выбери задачу, которую сделал. Потом пришли фото.\nЗона: <b>{group}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    context.user_data["mark_point"] = point
    context.user_data["mark_day"] = day
    context.user_data["mark_group"] = group
    return MARK_PICK_TASK


async def pick_task_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    idx = int(q.data.split(":", 1)[1])
    context.user_data["mark_idx"] = idx
    await q.edit_message_text("Теперь пришли 1 фото (после выполнения задачи).")
    return MARK_WAIT_PHOTO


async def mark_wait_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.message
    if not msg.photo:
        await msg.reply_text("Нужно именно фото. Пришли фото ещё раз.")
        return MARK_WAIT_PHOTO

    file_id = msg.photo[-1].file_id
    uid = update.effective_user.id
    users = load_users()
    name = user_name(users, uid)

    day = context.user_data.get("mark_day", today_key())
    point = context.user_data.get("mark_point")
    group = context.user_data.get("mark_group", "A")
    idx = int(context.user_data.get("mark_idx", -1))

    if not point or idx < 0:
        await msg.reply_text("Фото получено, но сейчас бот не ждёт фото.\nНажми: ✅ Отметить выполненное → выбери задачу → пришли фото.")
        return ConversationHandler.END

    state = load_cleaning().get(day_point_key(day, point), {})
    tasks = state.get("tasks") or ensure_tasks_loaded(day, point)
    split = state.get("split") or {}

    task_group = split.get(str(idx), "A")
    if group != "ALL" and task_group != group and not is_admin(uid):
        await msg.reply_text("⚠️ Это задача другой зоны. Закрывай только свои задачи.")
        return ConversationHandler.END

    mark_task_done(day, point, idx, uid, name, file_id)

    # Forward to Control group (photo + caption)
    if CONTROL_CHAT_ID:
        try:
            caption = (
                f"🧹 Уборка закрыта\n"
                f"Точка: {point}\n"
                f"Зона: {task_group}\n"
                f"Задача: {tasks[idx]}\n"
                f"Сотрудник: {name} ({uid})\n"
                f"Время: {now_tz().strftime('%Y-%m-%d %H:%M')}"
            )
            await context.bot.send_photo(chat_id=CONTROL_CHAT_ID, photo=file_id, caption=caption)
        except Exception as e:
            log.warning("Failed to forward cleaning photo to CONTROL: %s", e)

    # Clear violation if zone finished
    if not remaining_task_indices(day, point, group):
        v_clear_for_user(uid, "cleaning_pending")

    await msg.reply_text("✅ Принято. Задача закрыта с фото.")
    await msg.reply_text("Меню:", reply_markup=main_menu_kb(users.get(str(uid), {})))
    return ConversationHandler.END


# ----------------- TRANSFER POINT (close slot without fin report) -----------------

async def transfer_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    users = load_users()
    u = users.get(str(update.effective_user.id))
    if not u:
        await q.edit_message_text("Нужно /start")
        return ConversationHandler.END
    if not can_use_feature(u, "transfer"):
        await q.edit_message_text("Нет доступа.")
        return ConversationHandler.END

    slot = open_slot_for_user(update.effective_user.id)
    if not slot:
        await q.edit_message_text("У тебя нет открытого слота.")
        return ConversationHandler.END

    await q.edit_message_text("Напиши коротко: кому передал точку / комментарий по смене.")
    return TRANSFER_WAIT_COMMENT


async def transfer_wait_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = update.effective_user.id
    users = load_users()
    name = user_name(users, uid)

    slot = open_slot_for_user(uid)
    if not slot:
        await update.message.reply_text("Слот уже закрыт.")
        return ConversationHandler.END

    slots = load_slots()
    slot["status"] = "closed"
    slot["ended_at"] = now_tz().isoformat()
    slot["transfer_comment"] = text
    slots[slot["id"]] = slot
    save_slots(slots)

    if CONTROL_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=CONTROL_CHAT_ID,
                text=f"🔁 Передача точки: {slot['point']} | {name} ({uid}) | {slot['start']}-{slot['end']} | зона {slot.get('group')}\nКомментарий: {text}",
            )
        except Exception:
            pass

    await update.message.reply_text("✅ Ок, слот закрыт. Спасибо.")
    await update.message.reply_text("Меню:", reply_markup=main_menu_kb(users.get(str(uid), {})))
    return ConversationHandler.END


# ----------------- INCIDENT -----------------

async def incident_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    users = load_users()
    u = users.get(str(update.effective_user.id))
    if not u:
        await q.edit_message_text("Нужно /start")
        return ConversationHandler.END
    if not can_use_feature(u, "incident"):
        await q.edit_message_text("Нет доступа.")
        return ConversationHandler.END

    await q.edit_message_text("Напиши инцидент/комментарий (можно 1 сообщением). Если надо — потом пришли фото.")
    context.user_data["incident_waiting"] = True
    return INCIDENT_WAIT


async def incident_wait(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    users = load_users()
    name = user_name(users, uid)
    u = users.get(str(uid), {})
    point = u.get("point") or "(не выбрана)"

    txt = (update.message.text or "").strip()
    if CONTROL_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=CONTROL_CHAT_ID,
                text=f"💬 Инцидент/комментарий\nТочка: {point}\nСотрудник: {name} ({uid})\nТекст: {txt}",
            )
        except Exception:
            pass

    await update.message.reply_text("✅ Принято.")
    await update.message.reply_text("Меню:", reply_markup=main_menu_kb(u))
    context.user_data.pop("incident_waiting", None)
    return ConversationHandler.END


# ----------------- FIN REPORT (only after close) -----------------

async def fin_report_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    users = load_users()
    u = users.get(str(update.effective_user.id))
    if not u:
        await q.edit_message_text("Нужно /start")
        return ConversationHandler.END
    if not can_use_feature(u, "report"):
        await q.edit_message_text("Нет доступа.")
        return ConversationHandler.END

    point = u.get("point")
    if not point:
        await q.edit_message_text("Сначала выбери точку.", reply_markup=points_kb("set_point"))
        return ConversationHandler.END

    if not after_close_now(point):
        close_t = point_close_time(point).strftime("%H:%M")
        await q.edit_message_text(
            f"⛔ Фин. отчёт можно сдавать только после закрытия точки.\n"
            f"Для <b>{html_escape(point)}</b> это после <b>{close_t}</b>.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_kb(u),
        )
        return ConversationHandler.END

    day = today_key()
    context.user_data["r_point"] = point
    context.user_data["r_day"] = day
    context.user_data["r_photos"] = []

    await q.edit_message_text(f"Фин. отчёт ({html_escape(point)}). Введи сумму НАЛИЧКА (число).", parse_mode=ParseMode.HTML)
    return REPORT_CASH


async def report_cash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip().replace(" ", "").replace(",", ".")
    if not re.match(r"^\d+(\.\d+)?$", t):
        await update.message.reply_text("Нужно число. Например 12345")
        return REPORT_CASH
    context.user_data["r_cash"] = float(t)
    await update.message.reply_text("Введи сумму БЕЗНАЛ (число).")
    return REPORT_CASHLESS


async def report_cashless(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip().replace(" ", "").replace(",", ".")
    if not re.match(r"^\d+(\.\d+)?$", t):
        await update.message.reply_text("Нужно число. Например 12345")
        return REPORT_CASHLESS
    context.user_data["r_cashless"] = float(t)
    await update.message.reply_text("Пришли 1-2 фото чеков. Начни с первого фото.")
    return REPORT_PHOTO


async def report_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg.photo:
        await msg.reply_text("Нужно фото. Пришли фото чека.")
        return REPORT_PHOTO

    file_id = msg.photo[-1].file_id
    photos = context.user_data.get("r_photos", [])
    photos.append(file_id)
    context.user_data["r_photos"] = photos

    await msg.reply_text("✅ Принял. Если есть ещё фото — пришли. Если нет — напиши: ГОТОВО")
    return REPORT_PHOTO


async def report_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if (update.message.text or "").strip().lower() not in {"готово", "done", "ok"}:
        await update.message.reply_text("Если фото больше нет — напиши: ГОТОВО")
        return REPORT_PHOTO

    uid = update.effective_user.id
    users = load_users()
    name = user_name(users, uid)

    point = context.user_data.get("r_point")
    day = context.user_data.get("r_day", today_key())
    cash = float(context.user_data.get("r_cash", 0.0))
    cashless = float(context.user_data.get("r_cashless", 0.0))
    photos = context.user_data.get("r_photos", [])
    total = cash + cashless

    reports = load_reports()
    k = day_point_key(day, point)
    reports[k] = {
        "day": day,
        "point": point,
        "cash": cash,
        "cashless": cashless,
        "total": total,
        "photos": photos,
        "by": uid,
        "by_name": name,
        "ts": now_tz().isoformat(),
    }
    save_reports(reports)

    # close user's open slot if exists
    slot = open_slot_for_user(uid)
    if slot:
        slots = load_slots()
        slot["status"] = "closed"
        slot["ended_at"] = now_tz().isoformat()
        slot["closed_by_report"] = True
        slots[slot["id"]] = slot
        save_slots(slots)

    if CONTROL_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=CONTROL_CHAT_ID,
                text=(
                    f"🧾 Фин. отчёт: {point}\n"
                    f"Наличка: {cash:.2f}\n"
                    f"Безнал: {cashless:.2f}\n"
                    f"Итого: {total:.2f}\n"
                    f"Сдал: {name} ({uid})"
                ),
            )
        except Exception:
            pass

        # forward check photos too
        for i, fid in enumerate(photos, start=1):
            try:
                await context.bot.send_photo(chat_id=CONTROL_CHAT_ID, photo=fid, caption=f"🧾 Чек {i} | {point} | {day} | {name}")
            except Exception:
                pass

    await update.message.reply_text("✅ Фин. отчёт принят. Спасибо.")
    await update.message.reply_text("Меню:", reply_markup=main_menu_kb(users.get(str(uid), {})))
    return ConversationHandler.END


# ----------------- REMINDER JOB -----------------

async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    slots = load_slots()
    if not slots:
        return

    day = today_key()
    users = load_users()

    for slot_id, slot in list(slots.items()):
        if slot.get("status") != "open":
            continue

        uid = slot.get("user_id")
        point = slot.get("point")
        group = slot.get("group", "A")

        u = users.get(str(uid))
        if not u or not is_active_user(u):
            continue

        # only during point working hours (so we don't ping at night)
        if point and not is_within_point_hours(point):
            continue

        rem = remaining_task_indices(day, point, group)
        if rem:
            # First time: strict "косяк" once
            if not v_already_sent(slot_id, "cleaning_pending"):
                state = load_cleaning().get(day_point_key(day, point), {})
                tasks = state.get("tasks") or ensure_tasks_loaded(day, point)
                sample = [tasks[i] for i in rem[:5]]
                strict_msg = (
                    "⚠️ Косяк я снял (не закрыта уборка по твоей зоне) и доложу руководителю.\n"
                    f"Точка: {point}\n"
                    f"Зона: {group}\n"
                    "Осталось:\n• " + "\n• ".join(sample)
                )
                try:
                    await context.bot.send_message(chat_id=uid, text=strict_msg)
                except Exception:
                    pass
                v_set_sent(slot_id, "cleaning_pending", strict=True, value=True)

            # Then: gentle reminders every 30 min (no strict text repeats)
            state = load_cleaning().get(day_point_key(day, point), {})
            tasks = state.get("tasks") or ensure_tasks_loaded(day, point)
            sample = [tasks[i] for i in rem[:3]]
            gentle_msg = (
                f"⏰ Напоминание: осталось {len(rem)} задач по уборке (твоя зона {group}).\n"
                "Например:\n• " + "\n• ".join(sample)
            )
            try:
                await context.bot.send_message(chat_id=uid, text=gentle_msg)
            except Exception:
                pass
            v_set_sent(slot_id, "cleaning_pending", strict=False, value=True)
        else:
            # resolved -> clear flags
            v_set_sent(slot_id, "cleaning_pending", strict=True, value=False)
            v_set_sent(slot_id, "cleaning_pending", strict=False, value=False)


# ----------------- ADMIN -----------------

async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = update.effective_user.id
    if not is_admin(uid):
        await q.edit_message_text("Нет доступа.")
        return

    await q.edit_message_text(
        "Админка:\n"
        "/block <user_id> — заблокировать\n"
        "/unblock <user_id> — разблокировать\n"
        "/promote <user_id> — снять limited (полный доступ)\n"
        "/chatid — узнать chat_id группы\n",
    )


async def chatid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    await update.message.reply_text(f"chat_id: {chat.id}")


async def block_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Используй: /block 123456")
        return
    tid = context.args[0]
    users = load_users()
    u = users.get(str(tid))
    if not u:
        await update.message.reply_text("Пользователь не найден.")
        return
    u["active"] = False
    u["pending"] = False
    users[str(tid)] = u
    save_users(users)
    await update.message.reply_text(f"✅ Заблокирован {tid}")


async def unblock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Используй: /unblock 123456")
        return
    tid = context.args[0]
    users = load_users()
    u = users.get(str(tid))
    if not u:
        await update.message.reply_text("Пользователь не найден.")
        return
    u["active"] = True
    u["pending"] = False
    users[str(tid)] = u
    save_users(users)
    await update.message.reply_text(f"✅ Разблокирован {tid}")


async def promote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Используй: /promote 123456")
        return
    tid = context.args[0]
    users = load_users()
    u = users.get(str(tid))
    if not u:
        await update.message.reply_text("Пользователь не найден.")
        return
    u["limited"] = False
    u["active"] = True
    u["pending"] = False
    users[str(tid)] = u
    save_users(users)
    await update.message.reply_text(f"✅ Полный доступ выдан {tid}")


# ----------------- FALLBACK HANDLERS -----------------

async def photo_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Photo received when bot isn't waiting for a photo in a task/report flow."""
    msg: Message = update.message
    if not msg.photo:
        return
    file_id = msg.photo[-1].file_id
    uid = update.effective_user.id
    users = load_users()
    u = users.get(str(uid))
    name = user_name(users, uid)
    point = (u or {}).get("point") or "(не выбрана)"

    await msg.reply_text(
        "Фото получено, но сейчас бот не ждёт фото.\n"
        "Правильно так:\n"
        "1) ✅ Отметить выполненное\n"
        "2) Выбрать задачу\n"
        "3) Прислать фото"
    )

    # Still forward to Control as "unlinked photo" to avoid losing evidence
    if CONTROL_CHAT_ID:
        try:
            caption = (
                f"📷 Фото без привязки к задаче\n"
                f"Точка: {point}\n"
                f"Сотрудник: {name} ({uid})\n"
                f"Время: {now_tz().strftime('%Y-%m-%d %H:%M')}"
            )
            await context.bot.send_photo(chat_id=CONTROL_CHAT_ID, photo=file_id, caption=caption)
        except Exception:
            pass


async def text_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    users = load_users()
    u = users.get(str(update.effective_user.id))
    if u and is_active_user(u):
        await update.message.reply_text("Меню:", reply_markup=main_menu_kb(u))
    elif u and is_pending_user(u):
        await update.message.reply_text("⏳ Ты ожидаешь одобрения. Дождись сообщения от бота.")
    else:
        await update.message.reply_text("Нужно /start")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error("Unhandled exception", exc_info=context.error)
    # Don't spam users; only reply in private chats
    try:
        if isinstance(update, Update) and update.effective_chat and update.effective_chat.type == "private":
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Ошибка. Попробуй ещё раз.")
    except Exception:
        pass


# ----------------- BUILD APP -----------------

def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # Registration conversation
    reg_conv = ConversationHandler(
        entry_points=[CommandHandler("start", start_cmd)],
        states={
            REG_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_name)],
            REG_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_code)],
        },
        fallbacks=[],
    )

    # Slot conversation
    slot_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(open_slot_cb, pattern=r"^open_slot$")],
        states={
            SLOT_POINT: [CallbackQueryHandler(slot_point_cb, pattern=r"^slot_point:")],
            SLOT_MODE: [CallbackQueryHandler(slot_mode_cb, pattern=r"^slot_mode:")],
            SLOT_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, slot_time_text)],
        },
        fallbacks=[],
        per_message=False,
    )

    # Mark tasks conversation
    mark_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(mark_tasks_cb, pattern=r"^mark_tasks$")],
        states={
            MARK_PICK_TASK: [CallbackQueryHandler(pick_task_cb, pattern=r"^do_task:\d+$")],
            MARK_WAIT_PHOTO: [MessageHandler(filters.PHOTO, mark_wait_photo)],
        },
        fallbacks=[],
        per_message=False,
    )

    # Transfer point conversation
    transfer_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(transfer_point_cb, pattern=r"^transfer_point$")],
        states={TRANSFER_WAIT_COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, transfer_wait_comment)]},
        fallbacks=[],
        per_message=False,
    )

    # Incident conversation
    incident_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(incident_cb, pattern=r"^incident$")],
        states={INCIDENT_WAIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, incident_wait)]},
        fallbacks=[],
        per_message=False,
    )

    # Report conversation
    report_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(fin_report_cb, pattern=r"^fin_report$")],
        states={
            REPORT_CASH: [MessageHandler(filters.TEXT & ~filters.COMMAND, report_cash)],
            REPORT_CASHLESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, report_cashless)],
            REPORT_PHOTO: [
                MessageHandler(filters.PHOTO, report_photo),
                MessageHandler(filters.TEXT & ~filters.COMMAND, report_done),
            ],
        },
        fallbacks=[],
        per_message=False,
    )

    app.add_handler(reg_conv)

    # callbacks
    app.add_handler(CallbackQueryHandler(choose_point_cb, pattern=r"^choose_point$"))
    app.add_handler(CallbackQueryHandler(set_point_cb, pattern=r"^set_point:"))
    app.add_handler(slot_conv)
    app.add_handler(CallbackQueryHandler(plan_today_cb, pattern=r"^plan_today$"))
    app.add_handler(mark_conv)
    app.add_handler(transfer_conv)
    app.add_handler(report_conv)
    app.add_handler(incident_conv)

    # approval callbacks
    app.add_handler(CallbackQueryHandler(approve_user_cb, pattern=r"^approve_user:\d+$"))
    app.add_handler(CallbackQueryHandler(deny_user_cb, pattern=r"^deny_user:\d+$"))

    # admin
    app.add_handler(CallbackQueryHandler(admin_cb, pattern=r"^admin$"))
    app.add_handler(CommandHandler("chatid", chatid_cmd))
    app.add_handler(CommandHandler("block", block_cmd))
    app.add_handler(CommandHandler("unblock", unblock_cmd))
    app.add_handler(CommandHandler("promote", promote_cmd))

    # Photo fallback (must be AFTER conv handlers so it doesn't steal photos when bot is waiting)
    app.add_handler(MessageHandler(filters.PHOTO, photo_fallback))

    # Text fallback
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_fallback))

    # errors
    app.add_error_handler(error_handler)

    # job queue reminders
    if app.job_queue:
        app.job_queue.run_repeating(reminder_job, interval=REMINDER_INTERVAL_MIN * 60, first=30)
    else:
        log.warning("JobQueue not available. Ensure requirements include python-telegram-bot[job-queue].")

    return app


def main():
    start_health_server()
    app = build_app()
    log.info("Starting bot polling...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
