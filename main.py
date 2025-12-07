# -*- coding: utf-8 -*-
"""
WeCafe Cleaning & Shift Bot (Telegram)
- Self-registration by access code
- Admin approval in CONTROL group
- Slot-based cleaning tasks + photo proof after each task
- End-of-slot finance report + 1-2 receipt photos
- Reminders every N minutes + "Косяк снял..." after overdue
- Daily summary at END_OF_DAY_TIME to CONTROL group
Python: 3.12+
PTB: python-telegram-bot[job-queue]==21.6
"""

import os
import csv
import time
import sqlite3
import threading
import logging
from io import StringIO
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
from dotenv import load_dotenv

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    ConversationHandler,
    filters,
)


# -------------------- Render Health Server (IMPORTANT) --------------------
# Render Web Service требует открытый порт. Этот мини-сервер отвечает "ok" на любой GET.
def _start_health_server():
    port = int(os.getenv("PORT", "10000"))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, format, *args):
            return  # не засоряем логи

    HTTPServer(("0.0.0.0", port), Handler).serve_forever()

threading.Thread(target=_start_health_server, daemon=True).start()
# -------------------------------------------------------------------------

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("wecafe-bot")

# -------------------- Env --------------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

ADMIN_IDS = set()
_raw_admins = os.getenv("ADMIN_IDS", "").replace(" ", "")
if _raw_admins:
    for x in _raw_admins.split(","):
        if x.strip():
            ADMIN_IDS.add(int(x.strip()))

CONTROL_CHAT_ID = int(os.getenv("CONTROL_CHAT_ID", "0").strip() or "0")
ACCESS_CODE = os.getenv("ACCESS_CODE", "").strip()
SCHEDULE_CSV_URL = os.getenv("SCHEDULE_CSV_URL", "").strip()

# Norilsk timezone (Asia/Krasnoyarsk is correct for Norilsk)
try:
    TZ = ZoneInfo(os.getenv("TZ", "Asia/Krasnoyarsk").strip() or "Asia/Krasnoyarsk")
except Exception as e:
    logger.exception("TZ error, fallback to UTC: %s", e)
    TZ = ZoneInfo("UTC")

END_OF_DAY_TIME = os.getenv("END_OF_DAY_TIME", "22:30").strip() or "22:30"
REMINDER_INTERVAL_MIN = int(os.getenv("REMINDER_INTERVAL_MIN", "30").strip() or "30")

# Автоактивация (ограниченный доступ) только в это окно времени (Норильск)
AUTO_APPROVE_START = os.getenv("AUTO_APPROVE_START", "09:00").strip() or "09:00"
AUTO_APPROVE_END = os.getenv("AUTO_APPROVE_END", "15:00").strip() or "15:00"


POINTS = ["69 Параллель", "Арена", "Кафе Музей"]

# Рабочие часы точек (Норильск)
WORK_HOURS = {
    "69 Параллель": ("10:00", "22:00"),
    "Арена": ("10:00", "22:00"),
    "Кафе Музей": ("09:00", "19:00"),
}

# -------------------- DB --------------------
DB_PATH = "bot.db"
DB_LOCK = threading.Lock()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def now_ts() -> int:
    return int(time.time())


def now_dt() -> datetime:
    return datetime.now(TZ)


def parse_hhmm(s: str) -> dtime:
    hh, mm = s.split(":")
    return dtime(hour=int(hh), minute=int(mm))

def is_within_auto_window(dt: datetime) -> bool:
    """True if dt is within AUTO_APPROVE_START..AUTO_APPROVE_END in TZ."""
    start_t = parse_hhmm(AUTO_APPROVE_START)
    end_t = parse_hhmm(AUTO_APPROVE_END)
    start_dt = datetime.combine(dt.date(), start_t, TZ)
    end_dt = datetime.combine(dt.date(), end_t, TZ)
    return start_dt <= dt <= end_dt




def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def init_db():
    """Create all tables. Uses single-line SQL strings (no triple quotes)."""
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()

        cur.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            " tg_id INTEGER PRIMARY KEY,"
            " full_name TEXT NOT NULL,"
            " status TEXT NOT NULL,"
            " created_at INTEGER NOT NULL,"
            " approved_by INTEGER,"
            " last_point TEXT,"
            " pending_task_id INTEGER"
            ")"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS slots ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " tg_id INTEGER NOT NULL,"
            " point TEXT NOT NULL,"
            " start_ts INTEGER NOT NULL,"
            " planned_end_ts INTEGER NOT NULL,"
            " closed_ts INTEGER,"
            " status TEXT NOT NULL,"  # open/closed
            " last_reminder_ts INTEGER,"
            " last_koasyk_ts INTEGER,"
            " handoff_note TEXT"
            ")"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS slot_tasks ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " slot_id INTEGER NOT NULL,"
            " task_id TEXT NOT NULL,"
            " task_name TEXT NOT NULL,"
            " status TEXT NOT NULL,"  # pending/wait_photo/done
            " done_ts INTEGER,"
            " photo_file_id TEXT"
            ")"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS shift_totals ("
            " slot_id INTEGER PRIMARY KEY,"
            " deposit REAL,"
            " cash REAL,"
            " card REAL,"
            " total REAL,"
            " receipt_photo1 TEXT,"
            " receipt_photo2 TEXT,"
            " comment TEXT"
            ")"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS incidents ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " tg_id INTEGER NOT NULL,"
            " slot_id INTEGER,"
            " point TEXT,"
            " ts INTEGER NOT NULL,"
            " text TEXT NOT NULL,"
            " photo_file_id TEXT"
            ")"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS koasyk_events ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " slot_id INTEGER NOT NULL,"
            " ts INTEGER NOT NULL,"
            " reason TEXT NOT NULL"
            ")"
        )

        conn.commit()
        conn.close()


# -------------------- Schedule (Google Sheets CSV) --------------------
@dataclass
class ScheduleCache:
    fetched_at: float = 0.0
    rows: list[dict] | None = None


SCHEDULE_CACHE = ScheduleCache()


def fetch_schedule_rows() -> list[dict]:
    # Cache 5 minutes
    if SCHEDULE_CACHE.rows is not None and (time.time() - SCHEDULE_CACHE.fetched_at) < 300:
        return SCHEDULE_CACHE.rows

    if not SCHEDULE_CSV_URL:
        raise RuntimeError("SCHEDULE_CSV_URL is not set")

    r = requests.get(SCHEDULE_CSV_URL, timeout=25)

    if r.status_code in (401, 403):
        raise RuntimeError(
            "401/403: нет доступа к Google Sheets. Открой доступ: Share → Anyone with the link → Viewer. "
            "Или проверь SCHEDULE_CSV_URL (export?format=csv&gid=...)."
        )

    r.raise_for_status()

    # Декодирование (чтобы не было 'Ð¨ÐºÐ°ÑÑ...')
    raw = r.content
    csv_text = None
    for enc in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            csv_text = raw.decode(enc)
            break
        except Exception:
            continue
    if csv_text is None:
        csv_text = raw.decode("utf-8", errors="replace")
    reader = csv.DictReader(StringIO(csv_text))
    rows = list(reader)
    if not rows:
        raise RuntimeError("Schedule CSV is empty")

    norm_rows: list[dict] = []
    for row in rows:
        norm = {}
        for k, v in row.items():
            kk = k.strip() if isinstance(k, str) else k
            vv = v.strip() if isinstance(v, str) else v
            norm[kk] = vv
        norm_rows.append(norm)

    # Required minimal columns
    for col in ["task_id", "task_name", "point"]:
        if col not in norm_rows[0]:
            raise RuntimeError(f"Missing column in schedule: {col}")

    SCHEDULE_CACHE.rows = norm_rows
    SCHEDULE_CACHE.fetched_at = time.time()
    return norm_rows


def get_today_tasks(point: str) -> list[dict]:
    rows = fetch_schedule_rows()
    day = now_dt().day
    day_col = f"D{day}"

    def is_active(v) -> bool:
        if v is None:
            return False
        s = str(v).strip().lower()
        if s in ("1", "true", "yes", "y", "да"):
            return True
        return False

    tasks: list[dict] = []
    for row in rows:
        row_point = str(row.get("point", "")).strip()
        if row_point not in (point, "ALL"):
            continue

        if day_col not in row:
            continue
        if not is_active(row.get(day_col)):
            continue

        task_id = str(row.get("task_id", "")).strip() or "NA"
        name = str(row.get("task_name", "")).strip()
        if not name or name.lower() == "nan":
            name = f"(без названия) {task_id}"

        tasks.append({"task_id": task_id, "task_name": name})

    return tasks



def _point_hours(point: str) -> tuple[dtime, dtime]:
    hhmm_open, hhmm_close = WORK_HOURS.get(point, ("10:00", "22:00"))
    return parse_hhmm(hhmm_open), parse_hhmm(hhmm_close)


def _ts_today_at(t: dtime) -> int:
    dt = datetime.combine(now_dt().date(), t, TZ)
    return int(dt.timestamp())


def slot_create_custom(tg_id: int, point: str, start_ts: int, planned_end_ts: int) -> int:
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO slots (tg_id, point, start_ts, planned_end_ts, status) "
            "VALUES (?, ?, ?, ?, 'open')",
            (tg_id, point, int(start_ts), int(planned_end_ts)),
        )
        slot_id = cur.lastrowid
        conn.commit()
        conn.close()
        return slot_id


# -------------------- DB helpers --------------------
def user_get(tg_id: int):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,))
        row = cur.fetchone()
        conn.close()
        return row


def user_set_pending(tg_id: int, full_name: str):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (tg_id, full_name, status, created_at) "
            "VALUES (?, ?, 'pending', ?) "
            "ON CONFLICT(tg_id) DO UPDATE SET full_name=excluded.full_name, status='pending'",
            (tg_id, full_name, now_ts()),
        )
        conn.commit()
        conn.close()



def user_set_limited(tg_id: int, full_name: str):
    """Create/update user with status='limited' (ограниченный доступ)."""
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (tg_id, full_name, status, created_at) "
            "VALUES (?, ?, 'limited', ?) "
            "ON CONFLICT(tg_id) DO UPDATE SET full_name=excluded.full_name, status='limited'",
            (tg_id, full_name, now_ts()),
        )
        conn.commit()
        conn.close()


def user_approve(tg_id: int, approved_by: int):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE users SET status='active', approved_by=? WHERE tg_id=?", (approved_by, tg_id))
        conn.commit()
        conn.close()


def user_block(tg_id: int):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE users SET status='blocked' WHERE tg_id=?", (tg_id,))
        conn.commit()
        conn.close()


def user_unblock(tg_id: int):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE users SET status='active' WHERE tg_id=?", (tg_id,))
        conn.commit()
        conn.close()


def user_set_last_point(tg_id: int, point: str):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE users SET last_point=? WHERE tg_id=?", (point, tg_id))
        conn.commit()
        conn.close()


def user_set_pending_task(tg_id: int, slot_task_row_id: int | None):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE users SET pending_task_id=? WHERE tg_id=?", (slot_task_row_id, tg_id))
        conn.commit()
        conn.close()


def slot_get_open(tg_id: int):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM slots WHERE tg_id=? AND status='open' ORDER BY id DESC LIMIT 1", (tg_id,))
        row = cur.fetchone()
        conn.close()
        return row


def slot_create(tg_id: int, point: str, duration_minutes: int) -> int:
    start = now_ts()
    planned_end = start + duration_minutes * 60
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO slots (tg_id, point, start_ts, planned_end_ts, status) "
            "VALUES (?, ?, ?, ?, 'open')",
            (tg_id, point, start, planned_end),
        )
        slot_id = cur.lastrowid
        conn.commit()
        conn.close()
        return slot_id


def slot_close(slot_id: int):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE slots SET status='closed', closed_ts=? WHERE id=?", (now_ts(), slot_id))
        conn.commit()
        conn.close()


def slot_set_reminder_ts(slot_id: int, field: str, ts: int):
    if field not in ("last_reminder_ts", "last_koasyk_ts"):
        return
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute(f"UPDATE slots SET {field}=? WHERE id=?", (ts, slot_id))
        conn.commit()
        conn.close()


def slot_set_handoff(slot_id: int, note: str):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE slots SET handoff_note=? WHERE id=?", (note, slot_id))
        conn.commit()
        conn.close()


def slot_tasks_seed(slot_id: int, point: str):
    tasks = get_today_tasks(point)
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        for t in tasks:
            cur.execute(
                "INSERT INTO slot_tasks (slot_id, task_id, task_name, status) "
                "VALUES (?, ?, ?, 'pending')",
                (slot_id, t["task_id"], t["task_name"]),
            )
        conn.commit()
        conn.close()


def slot_tasks_list(slot_id: int):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM slot_tasks WHERE slot_id=? ORDER BY id ASC", (slot_id,))
        rows = cur.fetchall()
        conn.close()
        return rows


def slot_task_mark_wait_photo(slot_task_row_id: int):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("UPDATE slot_tasks SET status='wait_photo' WHERE id=?", (slot_task_row_id,))
        conn.commit()
        conn.close()


def slot_task_attach_photo_done(slot_task_row_id: int, photo_file_id: str):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            "UPDATE slot_tasks SET status='done', done_ts=?, photo_file_id=? WHERE id=?",
            (now_ts(), photo_file_id, slot_task_row_id),
        )
        conn.commit()
        conn.close()


def shift_totals_upsert(
    slot_id: int,
    deposit: float,
    cash: float,
    card: float,
    photo1: str,
    photo2: str | None,
    comment: str,
):
    total = cash + card
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO shift_totals (slot_id, deposit, cash, card, total, receipt_photo1, receipt_photo2, comment) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(slot_id) DO UPDATE SET "
            " deposit=excluded.deposit, cash=excluded.cash, card=excluded.card, total=excluded.total, "
            " receipt_photo1=excluded.receipt_photo1, receipt_photo2=excluded.receipt_photo2, comment=excluded.comment",
            (slot_id, deposit, cash, card, total, photo1, photo2, comment),
        )
        conn.commit()
        conn.close()


def incident_add(tg_id: int, slot_id: int | None, point: str | None, text: str, photo_file_id: str | None):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO incidents (tg_id, slot_id, point, ts, text, photo_file_id) VALUES (?, ?, ?, ?, ?, ?)",
            (tg_id, slot_id, point, now_ts(), text, photo_file_id),
        )
        conn.commit()
        conn.close()


def koasyk_add(slot_id: int, reason: str):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("INSERT INTO koasyk_events (slot_id, ts, reason) VALUES (?, ?, ?)", (slot_id, now_ts(), reason))
        conn.commit()
        conn.close()


# -------------------- UI --------------------
def employee_menu():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📍 Выбрать точку"), KeyboardButton("📋 План сегодня")],
            [KeyboardButton("▶️ Начать слот"), KeyboardButton("✅ Отметить выполненное")],
            [KeyboardButton("🔁 Передать точку"), KeyboardButton("⚠️ Инцидент/Комментарий")],
            [KeyboardButton("🧾 Фин. отчёт (закрыть слот)")],
        ],
        resize_keyboard=True,
    )


def limited_menu():
    """Меню для ограниченного доступа (автоактивация)."""
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📍 Выбрать точку"), KeyboardButton("📋 План сегодня")],
            [KeyboardButton("▶️ Начать слот"), KeyboardButton("✅ Отметить выполненное")],
        ],
        resize_keyboard=True,
    )


def menu_for_status(status: str):
    return limited_menu() if status == "limited" else employee_menu()



def admin_menu():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📊 Сводка сегодня"), KeyboardButton("🚨 Косяки/просрочки")],
            [KeyboardButton("👥 Сотрудники"), KeyboardButton("🧾 Отчёт недели")],
        ],
        resize_keyboard=True,
    )


def points_kb(prefix: str):
    return InlineKeyboardMarkup([[InlineKeyboardButton(p, callback_data=f"{prefix}:{p}")] for p in POINTS])


# -------------------- Conversation states --------------------
CLOSE_DEP, CLOSE_CASH, CLOSE_CARD, CLOSE_PHOTO1, CLOSE_PHOTO2, CLOSE_COMMENT = range(6)
HANDOFF_COMMENT = 10
SLOT_TIME_START = 20
SLOT_TIME_END = 21


# -------------------- Commands --------------------
async def cmd_chatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"chat_id = {update.effective_chat.id}")


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет прав.")
        return
    await update.message.reply_text("Админ-меню:", reply_markup=admin_menu())


async def cmd_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Использование: /block <tg_id>")
        return
    user_block(int(context.args[0]))
    await update.message.reply_text("Ок.")


async def cmd_unblock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Использование: /unblock <tg_id>")
        return
    user_unblock(int(context.args[0]))
    await update.message.reply_text("Ок.")


# -------------------- Core flow --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    row = user_get(u.id)

    if row and row["status"] in ("active", "limited"):
        if row["status"] == "limited":
            await update.message.reply_text(
                "Ты активирован в ограниченном доступе (автоактивация).\nДоступно: точка, план, отметки и фото.\nДля полного доступа попроси руководителя подтвердить.",
                reply_markup=limited_menu(),
            )
        else:
            await update.message.reply_text("Ты активен. Выбирай действие.", reply_markup=employee_menu())
            if is_admin(u.id):
                await update.message.reply_text("Админ-меню: /admin", reply_markup=admin_menu())
        return

    await update.message.reply_text("Привет. Введи своё имя (как в отчётах), одним сообщением.\nПример: Иван Петров")
    context.user_data.clear()
    context.user_data["reg_step"] = "name"


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    text = (update.message.text or "").strip()

    # Admin shortcuts via buttons
    if is_admin(u.id):
        if text == "📊 Сводка сегодня":
            await send_today_summary(context, to_chat=update.effective_chat.id)
            return
        if text == "🚨 Косяки/просрочки":
            await send_koasyk_today(context, to_chat=update.effective_chat.id)
            return
        if text == "👥 Сотрудники":
            await send_users_list(context, to_chat=update.effective_chat.id)
            return
        if text == "🧾 Отчёт недели":
            await send_week_report(context, to_chat=update.effective_chat.id)
            return

    # Registration steps
    if context.user_data.get("reg_step") == "name":
        context.user_data["reg_name"] = text
        context.user_data["reg_step"] = "code"
        await update.message.reply_text("Ок. Теперь введи код доступа.")
        return

    if context.user_data.get("reg_step") == "code":
        if text.strip() != ACCESS_CODE.strip():
            await update.message.reply_text("Код неверный. Попробуй ещё раз.")
            return

        full_name = context.user_data.get("reg_name", "Без имени")
        now = now_dt()

        # 09:00–15:00: автоактивация, но в ограниченном доступе
        if is_within_auto_window(now):
            user_set_limited(u.id, full_name)

            # Уведомление в Контроль + кнопки: снять ограничения / заблокировать
            if CONTROL_CHAT_ID != 0:
                try:
                    kb = InlineKeyboardMarkup([[
                        InlineKeyboardButton("✅ Снять ограничения", callback_data=f"appr:{u.id}"),
                        InlineKeyboardButton("⛔ Заблокировать", callback_data=f"rej:{u.id}"),
                    ]])
                    await context.bot.send_message(
                        CONTROL_CHAT_ID,
                        f"✅ Автоактивация (ограниченно, {AUTO_APPROVE_START}-{AUTO_APPROVE_END})\n"
                        f"• {full_name}\n• tg_id: {u.id}",
                        reply_markup=kb,
                    )
                except Exception as e:
                    logger.exception("Failed to notify CONTROL on auto-activation: %s", e)

            await update.message.reply_text(
                "✅ Готово. Ты активирован (ограниченный доступ).\n"
                "Доступно: выбрать точку, план, отметки и фото.",
                reply_markup=limited_menu(),
            )
            context.user_data.clear()
            return

        # Вне окна: обычная заявка на подтверждение
        user_set_pending(u.id, full_name)

        if CONTROL_CHAT_ID != 0:
            try:
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Одобрить", callback_data=f"appr:{u.id}"),
                    InlineKeyboardButton("⛔ Отклонить", callback_data=f"rej:{u.id}"),
                ]])
                await context.bot.send_message(
                    CONTROL_CHAT_ID,
                    f"Новая заявка сотрудника (вне окна автоактивации):\n• {full_name}\n• tg_id: {u.id}",
                    reply_markup=kb,
                )
            except Exception as e:
                logger.exception("Failed to send approval request: %s", e)
                await update.message.reply_text(
                    "✅ Код принят, но заявку в «Контроль» отправить не смог.\n"
                    "Проверь, что бот добавлен в группу и может писать."
                )
                context.user_data.clear()
                return

        await update.message.reply_text(
            f"⏰ Сейчас автоактивация не работает (только {AUTO_APPROVE_START}-{AUTO_APPROVE_END}).\n"
            "Заявка отправлена. Жди подтверждения.",
        )
        context.user_data.clear()
        return


    # Gate: only active employees can proceed
    row = user_get(u.id)
    if not row or row["status"] not in ("active", "limited"):
        await update.message.reply_text("Нет доступа. Напиши /start и пройди регистрацию.")
        return

    is_limited = (row["status"] == "limited")
    current_menu = limited_menu() if is_limited else employee_menu()

    # Incident free text flow
    if context.user_data.get("incident_mode"):
        context.user_data["incident_text"] = text
        context.user_data["incident_mode"] = False
        context.user_data["incident_wait_photo"] = True
        await update.message.reply_text("Если нужно — пришли фото. Если фото не нужно — напиши: без фото")
        return

    if context.user_data.get("incident_wait_photo") and text.lower().strip() == "без фото":
        open_slot = slot_get_open(u.id)
        point = open_slot["point"] if open_slot else row["last_point"]
        inc_text = context.user_data.get("incident_text", "(без текста)")

        incident_add(u.id, open_slot["id"] if open_slot else None, point, inc_text, None)

        context.user_data.pop("incident_wait_photo", None)
        context.user_data.pop("incident_text", None)

        # Notify control
        if CONTROL_CHAT_ID != 0:
            try:
                await context.bot.send_message(
                    CONTROL_CHAT_ID,
                    f"⚠️ Инцидент\nТочка: {point}\nСотрудник: {row['full_name']}\nТекст: {inc_text}",
                )
            except Exception:
                pass

        await update.message.reply_text("Инцидент записан.", reply_markup=employee_menu())
        return

    # Employee menu actions
    if text == "📍 Выбрать точку":
        await update.message.reply_text("Выбери точку:", reply_markup=points_kb("setpoint"))
        return

    if text == "📋 План сегодня":
        last_point = row["last_point"]
        if not last_point:
            await update.message.reply_text("Сначала выбери точку: 📍 Выбрать точку")
            return
        try:
            tasks = get_today_tasks(last_point)
        except Exception as e:
            await update.message.reply_text(f"Не могу прочитать график уборки. Проверь таблицу/ссылку.\nОшибка: {e}")
            return
        if not tasks:
            await update.message.reply_text("На сегодня задач не найдено (проверь таблицу).")
            return
        await update.message.reply_text(
            f"План на сегодня ({last_point}):\n" + "\n".join([f"• {t['task_name']}" for t in tasks[:100]]),
            reply_markup=employee_menu(),
        )
        return

    if text == "▶️ Начать слот":
        open_slot = slot_get_open(u.id)
        if open_slot:
            await update.message.reply_text("У тебя уже есть открытый слот. Работай по нему или закрой.", reply_markup=employee_menu())
            return
        await update.message.reply_text("Выбери точку для слота:", reply_markup=points_kb("point"))
        return

    if text == "✅ Отметить выполненное":
        open_slot = slot_get_open(u.id)
        if not open_slot:
            await update.message.reply_text("Нет активного слота. Нажми ▶️ Начать слот.")
            return
        if row["pending_task_id"]:
            await update.message.reply_text("Сначала отправь фото по предыдущей задаче.")
            return

        rows = slot_tasks_list(open_slot["id"])
        if not rows:
            await update.message.reply_text("На этот слот задач нет (проверь расписание на сегодня).")
            return

        buttons = []
        for r in rows:
            if r["status"] == "pending":
                buttons.append([InlineKeyboardButton(r["task_name"], callback_data=f"done:{r['id']}")])

        if not buttons:
            await update.message.reply_text("Пока нет задач для отметки (всё закрыто или ждёт фото).", reply_markup=employee_menu())
            return

        await update.message.reply_text("Выбери задачу, которую выполнил:", reply_markup=InlineKeyboardMarkup(buttons[:60]))
        return

    if text == "⚠️ Инцидент/Комментарий":
        if is_limited:
            await update.message.reply_text("⛔ Доступ ограничен. Инциденты доступны после подтверждения руководителем.", reply_markup=current_menu)
            return

        context.user_data["incident_mode"] = True
        await update.message.reply_text("Напиши текст инцидента/комментария одним сообщением. Потом можно фото.")
        return

    if text == "🔁 Передать точку":
        if is_limited:
            await update.message.reply_text("⛔ Доступ ограничен. Передача точки доступна после подтверждения руководителем.", reply_markup=current_menu)
            return

        open_slot = slot_get_open(u.id)
        if not open_slot:
            await update.message.reply_text("Нет активного слота. Сначала ▶️ Начать слот.")
            return
        context.user_data["close_slot_id"] = open_slot["id"]
        await update.message.reply_text("Передача точки: напиши коротко, что не сделано/на что обратить внимание:")
        return HANDOFF_COMMENT

    if text == "🧾 Фин. отчёт (закрыть слот)":
        if is_limited:
            await update.message.reply_text("⛔ Доступ ограничен. Финальный отчёт доступен после подтверждения руководителем.", reply_markup=current_menu)
            return

        open_slot = slot_get_open(u.id)
        if not open_slot:
            await update.message.reply_text("Нет активного слота.")
            return
        context.user_data["close_slot_id"] = open_slot["id"]
        context.user_data["handoff_note"] = None
        await update.message.reply_text("Введи ВНЕСЕНИЕ (число, можно 0):")
        return CLOSE_DEP

    await update.message.reply_text("Ок. Выбирай действие.", reply_markup=current_menu)


# -------------------- Callback queries --------------------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    u = update.effective_user

    # Admin approval
    if data.startswith("appr:") or data.startswith("rej:"):
        if not is_admin(u.id):
            await q.edit_message_text("Нет прав.")
            return
        tg_id = int(data.split(":")[1])

        if data.startswith("appr:"):
            user_approve(tg_id, u.id)
            await q.edit_message_text(f"✅ Одобрен tg_id={tg_id}")
            try:
                await context.bot.send_message(tg_id, "Ты активирован. Можно работать.", reply_markup=employee_menu())
            except Exception:
                pass
        else:
            user_block(tg_id)
            await q.edit_message_text(f"⛔ Отклонён tg_id={tg_id}")
        return

    # Set point (for plan, etc.)
    if data.startswith("setpoint:"):
        point = data.split(":", 1)[1]
        if point not in POINTS:
            await q.edit_message_text("Неверная точка.")
            return
        user_set_last_point(u.id, point)
        await q.edit_message_text(f"Точка выбрана: {point}\nТеперь можно открыть 📋 План сегодня.")
        return

    # Start slot flow
    if data.startswith("point:"):
        point = data.split(":", 1)[1]
        if point not in POINTS:
            await q.edit_message_text("Неверная точка.")
            return
        user_set_last_point(u.id, point)
        open_t, close_t = _point_hours(point)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🕘 Смена (до закрытия)", callback_data=f"full:{point}")],
            [InlineKeyboardButton("⏱ Ввести время (с/до)", callback_data=f"custom:{point}")],
        ])
        await q.edit_message_text(
            f"Точка: {point}\nЧасы: {open_t.strftime('%H:%M')}–{close_t.strftime('%H:%M')}\n\nВыбери вариант слота:",
            reply_markup=kb,
        )
        return
    if data.startswith("full:"):
        point = data.split(":", 1)[1]
        if point not in POINTS:
            await q.edit_message_text("Неверная точка.")
            return
        if slot_get_open(u.id):
            await q.edit_message_text("У тебя уже есть открытый слот.")
            return

        open_t, close_t = _point_hours(point)
        close_ts = _ts_today_at(close_t)
        now = now_ts()

        if now >= close_ts:
            await q.edit_message_text("Смена уже закончилась для этой точки. Если нужно — выбери время (с/до).")
            return

        slot_id = slot_create_custom(u.id, point, now, close_ts)
        try:
            slot_tasks_seed(slot_id, point)
        except Exception as e:
            await q.edit_message_text(f"Слот создан, но график уборки прочитать не смог. Ошибка: {e}")
            return

        await q.edit_message_text(
            f"Слот начат (до закрытия).\nТочка: {point}\nДо: {close_t.strftime('%H:%M')}\n\nДальше: ✅ Отметить выполненное"
        )
        try:
            await context.bot.send_message(u.id, "Меню:", reply_markup=employee_menu())
        except Exception:
            pass
        return

    if data.startswith("custom:"):
        point = data.split(":", 1)[1]
        if point not in POINTS:
            await q.edit_message_text("Неверная точка.")
            return
        if slot_get_open(u.id):
            await q.edit_message_text("У тебя уже есть открытый слот.")
            return

        context.user_data["slot_point"] = point
        open_t, close_t = _point_hours(point)

        await q.edit_message_text(
            f"Введи время НАЧАЛА слота (HH:MM)\nТочка: {point}\nЧасы: {open_t.strftime('%H:%M')}–{close_t.strftime('%H:%M')}"
        )
        return SLOT_TIME_START

    # Mark a task -> wait photo
    if data.startswith("done:"):
        row = user_get(u.id)
        if not row or row["status"] not in ("active", "limited"):
            await q.edit_message_text("Нет доступа.")
            return
        if row["pending_task_id"]:
            await q.edit_message_text("Сначала отправь фото по предыдущей задаче.")
            return

        slot_task_row_id = int(data.split(":")[1])
        slot_task_mark_wait_photo(slot_task_row_id)
        user_set_pending_task(u.id, slot_task_row_id)
        await q.edit_message_text("📷 Прикрепи фото по задаче одним сообщением.")
        return


# -------------------- Photos --------------------
async def on_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    row = user_get(u.id)
    if not row:
        await update.message.reply_text("Сначала /start")
        return

    file_id = update.message.photo[-1].file_id

    # Incident photo
    if context.user_data.get("incident_wait_photo"):
        open_slot = slot_get_open(u.id)
        point = open_slot["point"] if open_slot else row["last_point"]
        inc_text = context.user_data.get("incident_text", "(без текста)")

        incident_add(u.id, open_slot["id"] if open_slot else None, point, inc_text, file_id)

        context.user_data.pop("incident_wait_photo", None)
        context.user_data.pop("incident_text", None)

        if CONTROL_CHAT_ID != 0:
            try:
                await context.bot.send_message(
                    CONTROL_CHAT_ID,
                    f"⚠️ Инцидент\nТочка: {point}\nСотрудник: {row['full_name']}\n(см. фото)",
                )
                await context.bot.send_photo(CONTROL_CHAT_ID, file_id)
            except Exception:
                pass

        await update.message.reply_text("Инцидент записан.", reply_markup=employee_menu())
        return

    # Task photo
    pending_task = row["pending_task_id"]
    if pending_task:
        slot_task_attach_photo_done(pending_task, file_id)
        user_set_pending_task(u.id, None)
        await update.message.reply_text("✅ Задача закрыта (фото принято).", reply_markup=employee_menu())
        return

    # Close flow photos
    if context.user_data.get("close_wait") == "photo1":
        context.user_data["photo1"] = file_id
        context.user_data["close_wait"] = "photo2"
        await update.message.reply_text("Пришли 2-е фото чеков (если нужно) или напиши: пропустить")
        return CLOSE_PHOTO2

    if context.user_data.get("close_wait") == "photo2":
        context.user_data["photo2"] = file_id
        context.user_data["close_wait"] = "comment"
        await update.message.reply_text("Напиши комментарий по слоту (можно 'всё ок'):")
        return CLOSE_COMMENT
    await update.message.reply_text("Фото получено, но сейчас бот не ждёт фото.", reply_markup=employee_menu())


# -------------------- Close / Handoff conversation --------------------
async def handoff_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    note = (update.message.text or "").strip()
    slot_id = context.user_data.get("close_slot_id")
    if slot_id:
        slot_set_handoff(slot_id, note)
    context.user_data["handoff_note"] = note
    await update.message.reply_text("Ок. Теперь фин. отчёт. Введи ВНЕСЕНИЕ (число, можно 0):")
    return CLOSE_DEP


async def close_dep(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data["dep"] = float((update.message.text or "0").replace(",", ".").strip())
    except Exception:
        await update.message.reply_text("Нужно число. Введи внесение ещё раз:")
        return CLOSE_DEP
    await update.message.reply_text("Введи НАЛИЧКУ (число, можно 0):")
    return CLOSE_CASH


async def close_cash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data["cash"] = float((update.message.text or "0").replace(",", ".").strip())
    except Exception:
        await update.message.reply_text("Нужно число. Введи наличку ещё раз:")
        return CLOSE_CASH
    await update.message.reply_text("Введи БЕЗНАЛ (терминал) (число, можно 0):")
    return CLOSE_CARD


async def close_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data["card"] = float((update.message.text or "0").replace(",", ".").strip())
    except Exception:
        await update.message.reply_text("Нужно число. Введи безнал ещё раз:")
        return CLOSE_CARD

    context.user_data["close_wait"] = "photo1"
    await update.message.reply_text("Пришли 1-е фото чеков (обязательно):")
    return CLOSE_PHOTO1


async def close_photo2_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip().lower()
    if txt == "пропустить":
        context.user_data["photo2"] = None
        context.user_data["close_wait"] = "comment"
        await update.message.reply_text("Напиши комментарий по слоту (можно 'всё ок'):")
        return CLOSE_COMMENT

    await update.message.reply_text("Пришли фото или напиши: пропустить")
    return CLOSE_PHOTO2


async def close_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    comment = (update.message.text or "").strip() or "без комментария"
    slot_id = context.user_data.get("close_slot_id")
    if not slot_id:
        await update.message.reply_text("Ошибка: слот не найден.")
        context.user_data.clear()
        return ConversationHandler.END

    dep = float(context.user_data.get("dep", 0.0))
    cash = float(context.user_data.get("cash", 0.0))
    card = float(context.user_data.get("card", 0.0))
    photo1 = context.user_data.get("photo1")
    photo2 = context.user_data.get("photo2")
    handoff_note = context.user_data.get("handoff_note")

    if not photo1:
        await update.message.reply_text("Нужно 1-е фото чеков. Пришли фото:")
        context.user_data["close_wait"] = "photo1"
        return CLOSE_PHOTO1

    shift_totals_upsert(slot_id, dep, cash, card, photo1, photo2, comment)
    slot_close(slot_id)

    # Slot + task stats
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM slots WHERE id=?", (slot_id,))
        s = cur.fetchone()
        cur.execute("SELECT status, COUNT(*) c FROM slot_tasks WHERE slot_id=? GROUP BY status", (slot_id,))
        stats = {r["status"]: r["c"] for r in cur.fetchall()}
        conn.close()

    pending = int(stats.get("pending", 0))
    waitp = int(stats.get("wait_photo", 0))
    done = int(stats.get("done", 0))
    total_tasks = pending + waitp + done

    user_row = user_get(update.effective_user.id)
    msg = (
        "🧾 Слот закрыт\n"
        f"Точка: {s['point']}\n"
        f"Сотрудник: {user_row['full_name']}\n"
        f"Задачи: {done}/{total_tasks} (ожидают фото: {waitp}, не начаты: {pending})\n"
        f"Внесение: {dep:.0f}\n"
        f"Наличка: {cash:.0f}\n"
        f"Терминал: {card:.0f}\n"
        f"Итого: {(cash+card):.0f}\n"
        f"Комментарий: {comment}"
    )
    if handoff_note:
        msg += f"\nПередача точки: {handoff_note}"

    await update.message.reply_text("Слот закрыт. Спасибо.", reply_markup=employee_menu())

    if CONTROL_CHAT_ID != 0:
        try:
            await context.bot.send_message(CONTROL_CHAT_ID, msg)
            await context.bot.send_photo(CONTROL_CHAT_ID, photo1)
            if photo2:
                await context.bot.send_photo(CONTROL_CHAT_ID, photo2)
        except Exception:
            pass

    context.user_data.clear()
    return ConversationHandler.END


async def close_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Ок, отменил.", reply_markup=employee_menu())
    return ConversationHandler.END


# -------------------- Jobs (reminders / summary) --------------------
async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    # Find all open slots
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM slots WHERE status='open'")
        slots = cur.fetchall()
        conn.close()

    if not slots:
        return

    now = now_ts()
    for s in slots:
        tg_id = int(s["tg_id"])
        slot_id = int(s["id"])
        planned_end = int(s["planned_end_ts"])

        # Task stats
        with DB_LOCK:
            conn = db()
            cur = conn.cursor()
            cur.execute("SELECT status, COUNT(*) c FROM slot_tasks WHERE slot_id=? GROUP BY status", (slot_id,))
            stats = {r["status"]: int(r["c"]) for r in cur.fetchall()}
            conn.close()

        pending = int(stats.get("pending", 0))
        waitp = int(stats.get("wait_photo", 0))

        # Regular reminder
        last_rem = int(s["last_reminder_ts"] or 0)
        if now - last_rem >= REMINDER_INTERVAL_MIN * 60:
            if pending > 0 or waitp > 0:
                try:
                    await context.bot.send_message(
                        tg_id,
                        f"⏰ Напоминание: осталось задач: {pending}. Ожидают фото: {waitp}.",
                    )
                except Exception:
                    pass
            slot_set_reminder_ts(slot_id, "last_reminder_ts", now)

        # Overdue -> "Косяк снял..."
        if now > planned_end:
            reasons = []
            if pending > 0:
                reasons.append(f"не закрыты задачи ({pending})")
            if waitp > 0:
                reasons.append(f"нет фото по задачам ({waitp})")
            reasons.append("слот не закрыт")
            reason = ", ".join(reasons)

            last_k = int(s["last_koasyk_ts"] or 0)
            if now - last_k >= REMINDER_INTERVAL_MIN * 60:
                try:
                    await context.bot.send_message(tg_id, f"Косяк снял: {reason}. Доложу руководителю.")
                except Exception:
                    pass

                koasyk_add(slot_id, reason)

                if CONTROL_CHAT_ID != 0:
                    ur = user_get(tg_id)
                    name = ur["full_name"] if ur else str(tg_id)
                    try:
                        await context.bot.send_message(
                            CONTROL_CHAT_ID,
                            f"⚠️ Косяк\nТочка: {s['point']}\nСотрудник: {name}\nПричина: {reason}\nПросрочка: {int((now - planned_end)/60)} мин",
                        )
                    except Exception:
                        pass

                slot_set_reminder_ts(slot_id, "last_koasyk_ts", now)


async def daily_summary_job(context: ContextTypes.DEFAULT_TYPE):
    if CONTROL_CHAT_ID != 0:
        await send_today_summary(context, to_chat=CONTROL_CHAT_ID)


# -------------------- Reports --------------------
async def send_today_summary(context: ContextTypes.DEFAULT_TYPE, to_chat: int):
    today = now_dt().date()
    start = int(datetime.combine(today, dtime(0, 0), TZ).timestamp())
    end = int(datetime.combine(today + timedelta(days=1), dtime(0, 0), TZ).timestamp())

    with DB_LOCK:
        conn = db()
        cur = conn.cursor()

        cur.execute(
            "SELECT s.*, u.full_name "
            "FROM slots s "
            "LEFT JOIN users u ON u.tg_id = s.tg_id "
            "WHERE s.start_ts >= ? AND s.start_ts < ?",
            (start, end),
        )
        slots = cur.fetchall()

        cur.execute(
            "SELECT s.point, SUM(t.deposit) dep, SUM(t.cash) cash, SUM(t.card) card, SUM(t.total) total "
            "FROM shift_totals t "
            "JOIN slots s ON s.id = t.slot_id "
            "WHERE s.start_ts >= ? AND s.start_ts < ? "
            "GROUP BY s.point",
            (start, end),
        )
        sums = {r["point"]: r for r in cur.fetchall()}
        conn.close()

    by_point = {p: [] for p in POINTS}
    for s in slots:
        by_point.setdefault(s["point"], []).append(s)

    lines = [f"📊 Сводка дня {today.isoformat()}"]
    for p in POINTS:
        lst = by_point.get(p, [])
        closed = sum(1 for s in lst if s["status"] == "closed")
        open_ = sum(1 for s in lst if s["status"] == "open")

        sumrow = sums.get(p)
        if sumrow:
            total = float(sumrow["total"] or 0)
            cash = float(sumrow["cash"] or 0)
            card = float(sumrow["card"] or 0)
            dep = float(sumrow["dep"] or 0)
            money = f"Внес: {dep:.0f} | Нал: {cash:.0f} | Тер: {card:.0f} | Итого: {total:.0f}"
        else:
            money = "Финансы: нет данных"

        lines.append(f"\n{p}")
        lines.append(f"Слоты: закрыто {closed}, открыто {open_}")
        lines.append(money)

    await context.bot.send_message(to_chat, "\n".join(lines))


async def send_koasyk_today(context: ContextTypes.DEFAULT_TYPE, to_chat: int):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            "SELECT s.*, u.full_name "
            "FROM slots s "
            "LEFT JOIN users u ON u.tg_id = s.tg_id "
            "WHERE s.status='open'"
        )
        slots = cur.fetchall()
        conn.close()

    if not slots:
        await context.bot.send_message(to_chat, "🚨 Сейчас нет открытых слотов.")
        return

    now = now_ts()
    lines = ["🚨 Открытые слоты / просрочки:"]
    for s in slots:
        overdue_min = max(0, int((now - int(s["planned_end_ts"])) / 60))
        name = s["full_name"] or str(s["tg_id"])
        tag = f"просрочка {overdue_min} мин" if overdue_min > 0 else "в работе"
        lines.append(f"• {s['point']} — {name} — {tag}")

    await context.bot.send_message(to_chat, "\n".join(lines))


async def send_users_list(context: ContextTypes.DEFAULT_TYPE, to_chat: int):
    with DB_LOCK:
        conn = db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT 200")
        rows = cur.fetchall()
        conn.close()

    if not rows:
        await context.bot.send_message(to_chat, "Пользователей нет.")
        return

    lines = ["👥 Сотрудники:"]
    for r in rows:
        lines.append(f"• {r['full_name']} — {r['status']} — tg_id {r['tg_id']}")
    lines.append("\n/block <tg_id> — заблокировать\n/unblock <tg_id> — разблокировать")

    await context.bot.send_message(to_chat, "\n".join(lines))


async def send_week_report(context: ContextTypes.DEFAULT_TYPE, to_chat: int):
    today = now_dt().date()
    start_date = today - timedelta(days=7)
    start = int(datetime.combine(start_date, dtime(0, 0), TZ).timestamp())
    end = int(datetime.combine(today + timedelta(days=1), dtime(0, 0), TZ).timestamp())

    with DB_LOCK:
        conn = db()
        cur = conn.cursor()

        cur.execute(
            "SELECT s.tg_id, u.full_name, "
            " SUM(CASE WHEN st.status='done' THEN 1 ELSE 0 END) AS done_tasks, "
            " COUNT(*) AS total_tasks, "
            " SUM(CASE WHEN st.status='wait_photo' THEN 1 ELSE 0 END) AS wait_photo "
            "FROM slots s "
            "JOIN slot_tasks st ON st.slot_id = s.id "
            "LEFT JOIN users u ON u.tg_id = s.tg_id "
            "WHERE s.start_ts >= ? AND s.start_ts < ? "
            "GROUP BY s.tg_id",
            (start, end),
        )
        task_rows = {int(r["tg_id"]): r for r in cur.fetchall()}

        cur.execute(
            "SELECT s.tg_id, u.full_name, "
            " SUM(COALESCE(t.total,0)) AS revenue, "
            " SUM((COALESCE(s.closed_ts, s.planned_end_ts) - s.start_ts)) AS seconds "
            "FROM slots s "
            "LEFT JOIN shift_totals t ON t.slot_id = s.id "
            "LEFT JOIN users u ON u.tg_id = s.tg_id "
            "WHERE s.start_ts >= ? AND s.start_ts < ? "
            "GROUP BY s.tg_id",
            (start, end),
        )
        rev_rows = cur.fetchall()

        cur.execute(
            "SELECT s.tg_id, COUNT(*) AS koasyk_cnt "
            "FROM koasyk_events k "
            "JOIN slots s ON s.id = k.slot_id "
            "WHERE s.start_ts >= ? AND s.start_ts < ? "
            "GROUP BY s.tg_id",
            (start, end),
        )
        ko_rows = {int(r["tg_id"]): int(r["koasyk_cnt"]) for r in cur.fetchall()}
        conn.close()

    combined = []
    for r in rev_rows:
        tg_id = int(r["tg_id"])
        name = r["full_name"] or str(tg_id)
        revenue = float(r["revenue"] or 0.0)
        seconds = float(r["seconds"] or 0.0)
        hours = seconds / 3600.0 if seconds > 0 else 0.0
        revph = revenue / hours if hours > 0 else 0.0

        tr = task_rows.get(tg_id)
        done_tasks = int(tr["done_tasks"]) if tr else 0
        total_tasks = int(tr["total_tasks"]) if tr else 0
        disc = (done_tasks / total_tasks * 100.0) if total_tasks > 0 else 0.0

        koasyk_cnt = int(ko_rows.get(tg_id, 0))

        combined.append(
            {"name": name, "revph": revph, "disc": disc, "done": done_tasks, "total": total_tasks, "koasyk": koasyk_cnt}
        )

    if not combined:
        await context.bot.send_message(to_chat, "🧾 Отчёт недели: нет данных.")
        return

    top_disc = sorted(combined, key=lambda x: (x["disc"], -x["koasyk"]), reverse=True)[:10]
    top_rev = sorted(combined, key=lambda x: x["revph"], reverse=True)[:10]
    worst = sorted(combined, key=lambda x: (x["koasyk"], -x["disc"]), reverse=True)[:10]

    lines = [f"🧾 Отчёт недели ({start_date} — {today})", "\nТОП дисциплина"]
    for x in top_disc:
        lines.append(f"• {x['name']}: {x['disc']:.0f}% ({x['done']}/{x['total']}), косяков: {x['koasyk']}")

    lines.append("\nТОП выручка/час")
    for x in top_rev:
        lines.append(f"• {x['name']}: {x['revph']:.0f} ₽/час")

    lines.append("\nПроблемные (косяки)")
    for x in worst:
        lines.append(f"• {x['name']}: косяков {x['koasyk']}, дисциплина {x['disc']:.0f}%")

    await context.bot.send_message(to_chat, "\n".join(lines))



# -------------------- Custom slot time flow --------------------
async def slot_time_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    point = context.user_data.get("slot_point")
    if not point:
        await update.message.reply_text("Ошибка: точка не выбрана. Нажми ▶️ Начать слот заново.", reply_markup=employee_menu())
        context.user_data.pop("slot_point", None)
        return ConversationHandler.END

    txt = (update.message.text or "").strip()
    try:
        t_start = parse_hhmm(txt)
    except Exception:
        await update.message.reply_text("Нужно время в формате HH:MM. Пример: 10:00")
        return SLOT_TIME_START

    open_t, close_t = _point_hours(point)
    if t_start < open_t or t_start > close_t:
        await update.message.reply_text(f"Начало должно быть внутри часов точки: {open_t.strftime('%H:%M')}–{close_t.strftime('%H:%M')}")
        return SLOT_TIME_START

    start_ts = _ts_today_at(t_start)
    # Не даём старт в далёком будущем
    if start_ts > now_ts() + 10 * 60:
        await update.message.reply_text("Начало получилось в будущем. Начни слот ближе к старту и введи время ещё раз.")
        return SLOT_TIME_START

    context.user_data["slot_start_ts"] = start_ts
    await update.message.reply_text("Теперь введи время ОКОНЧАНИЯ слота (HH:MM). Пример: 22:00")
    return SLOT_TIME_END


async def slot_time_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    point = context.user_data.get("slot_point")
    start_ts = context.user_data.get("slot_start_ts")
    if not point or not start_ts:
        await update.message.reply_text("Ошибка: не хватает данных. Нажми ▶️ Начать слот заново.", reply_markup=employee_menu())
        context.user_data.pop("slot_point", None)
        context.user_data.pop("slot_start_ts", None)
        return ConversationHandler.END

    txt = (update.message.text or "").strip()
    try:
        t_end = parse_hhmm(txt)
    except Exception:
        await update.message.reply_text("Нужно время в формате HH:MM. Пример: 22:00")
        return SLOT_TIME_END

    open_t, close_t = _point_hours(point)
    if t_end < open_t or t_end > close_t:
        await update.message.reply_text(f"Окончание должно быть внутри часов точки: {open_t.strftime('%H:%M')}–{close_t.strftime('%H:%M')}")
        return SLOT_TIME_END

    end_ts = _ts_today_at(t_end)
    if end_ts <= int(start_ts):
        await update.message.reply_text("Окончание должно быть позже начала. Введи окончание ещё раз:")
        return SLOT_TIME_END

    if now_ts() >= end_ts:
        await update.message.reply_text("Окончание уже в прошлом. Проверь время и введи окончание ещё раз:")
        return SLOT_TIME_END

    # Create slot
    # Protect against duplicate open slot
    if slot_get_open(update.effective_user.id):
        await update.message.reply_text("У тебя уже есть открытый слот.", reply_markup=employee_menu())
        context.user_data.clear()
        return ConversationHandler.END

    slot_id = slot_create_custom(update.effective_user.id, point, int(start_ts), int(end_ts))
    try:
        slot_tasks_seed(slot_id, point)
    except Exception as e:
        await update.message.reply_text(f"Слот создан, но график уборки прочитать не смог. Ошибка: {e}", reply_markup=employee_menu())
        context.user_data.clear()
        return ConversationHandler.END

    await update.message.reply_text(
        f"Слот начат.\\nТочка: {point}\\nС: {datetime.fromtimestamp(int(start_ts), TZ).strftime('%H:%M')}\\nДо: {datetime.fromtimestamp(int(end_ts), TZ).strftime('%H:%M')}\\n\\nДальше: ✅ Отметить выполненное",
        reply_markup=employee_menu(),
    )
    context.user_data.clear()
    return ConversationHandler.END


# -------------------- Error handler --------------------
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error: %s", context.error)


# -------------------- Build app --------------------
def build_app() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is required (.env)")

    app = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex(r"^🧾 Фин\. отчёт \(закрыть слот\)$"), on_text),
            MessageHandler(filters.Regex(r"^🔁 Передать точку$"), on_text),
        ],
        states={
            HANDOFF_COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handoff_comment)],
            CLOSE_DEP: [MessageHandler(filters.TEXT & ~filters.COMMAND, close_dep)],
            CLOSE_CASH: [MessageHandler(filters.TEXT & ~filters.COMMAND, close_cash)],
            CLOSE_CARD: [MessageHandler(filters.TEXT & ~filters.COMMAND, close_card)],
            CLOSE_PHOTO1: [MessageHandler(filters.PHOTO, on_photo)],
            CLOSE_PHOTO2: [
                MessageHandler(filters.PHOTO, on_photo),
                MessageHandler(filters.TEXT & ~filters.COMMAND, close_photo2_text),
            ],
            CLOSE_COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, close_comment)],
        },
        fallbacks=[CommandHandler("cancel", close_cancel)],
        allow_reentry=True,
    )


    slot_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(on_callback, pattern=r"^custom:")],
        states={
            SLOT_TIME_START: [MessageHandler(filters.TEXT & ~filters.COMMAND, slot_time_start)],
            SLOT_TIME_END: [MessageHandler(filters.TEXT & ~filters.COMMAND, slot_time_end)],
        },
        fallbacks=[CommandHandler("cancel", close_cancel)],
        allow_reentry=True,
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("block", cmd_block))
    app.add_handler(CommandHandler("unblock", cmd_unblock))
    app.add_handler(CommandHandler("chatid", cmd_chatid))

    app.add_handler(slot_conv)
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(conv)

    app.add_handler(MessageHandler(filters.PHOTO, on_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.add_error_handler(on_error)

    # Jobs
    if app.job_queue is None:
        raise RuntimeError('JobQueue not available. Install: pip install "python-telegram-bot[job-queue]==21.6"')

    app.job_queue.run_repeating(reminder_job, interval=REMINDER_INTERVAL_MIN * 60, first=30)
    app.job_queue.run_daily(daily_summary_job, time=parse_hhmm(END_OF_DAY_TIME))

    return app


def main():
    init_db()
    build_app().run_polling(close_loop=False)


if __name__ == "__main__":
    main()
