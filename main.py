import asyncio
import datetime
import email
import re
from email.header import decode_header
from email.utils import parseaddr
import mimetypes
import os
import smtplib
import sqlite3
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aioimaplib import aioimaplib

# ==================== НАСТРОЙКИ ПРОЕКТА ====================
TG_TOKEN = "API_ВАШЕГО_БОТА"
MODERATOR_CHAT_ID = -0  # ВПИШИ СЮДА ID СВОЕГО ЧАТА

IMAP_SERVER = "ВАШ_IMAP_СЕРВЕР"
IMAP_PORT = 993
EMAIL_USER = "ВАША_ПОЧТА"
EMAIL_PASSWORD = "ПАРОЛЬ_ПОЧТЫ"

SMTP_SERVER = "ВАШ_SMTP_СЕРВЕР"
SMTP_PORT = 465
# ============================================================

# Абсолютные пути, чтобы бот работал из любой директории
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "support_bot.db")
LOGO_PATH = os.path.join(BASE_DIR, "assets", "logo.png")

# Лог в файл: всё, что видно в консоли, плюс отметка времени
LOG_PATH = os.path.join(BASE_DIR, "log.txt")
LOG_MAX_BYTES = 1024 * 1024


def _log(msg):
    line = f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line)
    try:
        if os.path.exists(LOG_PATH) and os.path.getsize(LOG_PATH) > LOG_MAX_BYTES:
            os.replace(LOG_PATH, LOG_PATH + ".old")
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as exc:
        print(f"[log] Не удалось записать в log.txt: {exc}")

# Автоочистка базы: сколько дней хранить заявки и как часто проверять
DB_RETENTION_DAYS = 30
DB_CLEANUP_INTERVAL = 24 * 60 * 60

bot = Bot(token=TG_TOKEN)
dp = Dispatcher()

# --- БАЗА ДАННЫХ SQLITE ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT,
            tg_message_id INTEGER UNIQUE,
            is_replied INTEGER DEFAULT 0,
            created_at TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS blacklist (
            email TEXT PRIMARY KEY COLLATE NOCASE,
            reason TEXT DEFAULT '',
            added_by TEXT DEFAULT '',
            added_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _migrate_tickets(cursor)
    _migrate_created_at(cursor)
    conn.commit()
    conn.close()


def _migrate_created_at(cursor):
    """Добавляет дату создания заявки, если её ещё нет, и триггер для новых записей."""
    cols = [row[1] for row in cursor.execute("PRAGMA table_info(tickets)")]
    added = False
    if "created_at" not in cols:
        cursor.execute("ALTER TABLE tickets ADD COLUMN created_at TEXT")
        cursor.execute(
            "UPDATE tickets SET created_at = datetime('now','localtime') WHERE created_at IS NULL"
        )
        added = True
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS tickets_stamp_created_at
        AFTER INSERT ON tickets
        WHEN NEW.created_at IS NULL
        BEGIN
            UPDATE tickets SET created_at = datetime('now','localtime')
            WHERE ticket_id = NEW.ticket_id;
        END
    """)
    if added:
        _log("База: добавлена колонка created_at для автоочистки.")
    return added


def cleanup_old_tickets(days=None):
    """Удаляет заявки старше указанного срока. Возвращает число удалённых."""
    days = DB_RETENTION_DAYS if days is None else days
    if days <= 0:
        return 0
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM tickets WHERE created_at IS NOT NULL AND created_at != '' "
        "AND created_at < datetime('now','localtime', ?)",
        (f"-{int(days)} days",),
    )
    deleted = cursor.rowcount
    conn.commit()
    if deleted:
        try:
            cursor.execute("VACUUM")
        except sqlite3.Error as e:
            _log(f"[Очистка] VACUUM не удался: {e}")
    conn.close()
    return deleted


async def db_cleanup_loop():
    """Раз в сутки чистит базу от заявок старше DB_RETENTION_DAYS."""
    while True:
        try:
            deleted = cleanup_old_tickets()
            if deleted:
                _log(f"[Очистка] Удалено заявок старше {DB_RETENTION_DAYS} дн.: {deleted}")
        except Exception as e:
            _log(f"[Очистка] Ошибка чистки базы: {e}")
        await asyncio.sleep(DB_CLEANUP_INTERVAL)


def format_ticket_no(number):
    """Номер заявки в виде 00001. В базе номер растёт сам и после чистки не сбрасывается."""
    try:
        return f"{int(number):05d}"
    except (TypeError, ValueError):
        return str(number)


def _migrate_tickets(cursor):
    """Старая база могла создать tickets без ticket_id — переносим данные в новую схему."""
    cols = [row[1] for row in cursor.execute("PRAGMA table_info(tickets)")]
    if not cols or "ticket_id" in cols:
        return False

    cursor.execute("ALTER TABLE tickets RENAME TO tickets_old")
    cursor.execute("""
        CREATE TABLE tickets (
            ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email TEXT,
            tg_message_id INTEGER UNIQUE,
            is_replied INTEGER DEFAULT 0
        )
    """)

    old = [row[1] for row in cursor.execute("PRAGMA table_info(tickets_old)")]

    def pick(*names):
        for name in names:
            if name in old:
                return name
        return None

    email_col = pick("user_email", "email", "sender")
    tg_col = pick("tg_message_id", "message_id", "msg_id")
    replied_col = pick("is_replied", "replied")

    if tg_col:
        cursor.execute(
            "INSERT INTO tickets (user_email, tg_message_id, is_replied) "
            f"SELECT {email_col or 'NULL'}, {tg_col}, {f'COALESCE({replied_col}, 0)' if replied_col else '0'} "
            "FROM tickets_old WHERE tg_message_id IS NOT NULL ORDER BY rowid"
        )
    cursor.execute("DROP TABLE tickets_old")
    _log(f"База обновлена: {len(cols)} старых колонок → ticket_id.")
    return True

def save_ticket(tg_msg_id, user_email):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO tickets (user_email, tg_message_id) VALUES (?, ?)", (user_email, tg_msg_id))
    conn.commit()
    cursor.execute("SELECT ticket_id FROM tickets WHERE tg_message_id = ?", (tg_msg_id,))
    row = cursor.fetchone()
    ticket_id = row[0] if row else 1
    conn.close()
    return format_ticket_no(ticket_id)

def get_ticket_info(tg_msg_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT user_email, ticket_id, is_replied FROM tickets WHERE tg_message_id = ?", (tg_msg_id,))
    row = cursor.fetchone()
    conn.close()
    if row:
        return row[0], format_ticket_no(row[1]), row[2]
    return None, None, None

def mark_ticket_as_replied(tg_msg_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("UPDATE tickets SET is_replied = 1 WHERE tg_message_id = ?", (tg_msg_id,))
    conn.commit()
    conn.close()

# --- ЧЁРНЫЙ СПИСОК (ЧС) ---
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def is_blacklisted(email):
    email = (email or "").strip().lower()
    if "@" not in email:
        return False
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute("SELECT 1 FROM blacklist WHERE email = ?", (email,)).fetchone()
    finally:
        conn.close()
    return row is not None


def add_to_blacklist(email, reason="", added_by=""):
    """True — адрес добавлен впервые, False — причина обновлена."""
    email = (email or "").strip().lower()
    conn = sqlite3.connect(DB_PATH)
    try:
        existed = conn.execute("SELECT 1 FROM blacklist WHERE email = ?", (email,)).fetchone() is not None
        conn.execute(
            "INSERT INTO blacklist (email, reason, added_by) VALUES (?, ?, ?) "
            "ON CONFLICT(email) DO UPDATE SET reason = excluded.reason, "
            "added_by = excluded.added_by, added_at = CURRENT_TIMESTAMP",
            (email, reason or "", added_by or ""),
        )
        conn.commit()
    finally:
        conn.close()
    return not existed


def remove_from_blacklist(email):
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.execute("DELETE FROM blacklist WHERE email = ?", ((email or "").strip().lower(),))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def list_blacklist(limit=100):
    conn = sqlite3.connect(DB_PATH)
    try:
        return conn.execute(
            "SELECT email, reason, added_by, added_at FROM blacklist "
            "ORDER BY added_at DESC, email LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()


def decode_mime_header(header_value):
    if not header_value:
        return "Без темы"
    decoded = decode_header(header_value)
    parts = []
    for text, encoding in decoded:
        if isinstance(text, bytes):
            parts.append(text.decode(encoding or "utf-8", errors="ignore"))
        else:
            parts.append(str(text))
    return "".join(parts)

# --- АВТОМАТИЧЕСКОЕ ПОДТВЕРЖДЕНИЕ ЗАЯВКИ ПОЛЬЗОВАТЕЛЮ ---
def send_auto_confirmation_email(user_email, ticket_number):
    msg = MIMEMultipart("related")
    msg["Subject"] = f"Ваша заявка №{ticket_number} принята — Поддержка BrokenVK"
    msg["From"] = EMAIL_USER
    msg["To"] = user_email

    msg_alternative = MIMEMultipart("alternative")
    msg.attach(msg_alternative)

    html_template = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333333; line-height: 1.6; padding: 20px; text-align: center;">
        <div style="margin-bottom: 25px;">
          <img src="cid:project_logo" alt="BrokenVK" style="max-width: 220px; height: auto;" />
        </div>
        <div style="background-color: #f4f6f9; border-top: 4px solid #4caf50; padding: 20px; margin-bottom: 25px; border-radius: 4px; display: inline-block; text-align: left; min-width: 350px;">
          <h3 style="color: #4caf50; margin-top: 0; font-size: 18px;">Ваша заявка успешно принята!</h3>
          <p style="font-size: 15px; margin: 10px 0;">Ей присвоен уникальный номер: <strong style="color: #5181b8;">№{ticket_number}</strong></p>
          <p style="color: #555555; font-size: 14px; margin: 0 0 15px 0;">Наши модераторы уже изучают ваше обращение и ответят вам в ближайшее время. Пожалуйста, не создавайте повторных запросов, также если вы отправили файл, убедитесь в том что он не весит больше 50 МБ, иначе не примем</p>
        </div>
        <hr style="border: 0; border-top: 1px solid #eeeeee; margin: 20px 0;">
        <div style="font-size: 13px; color: #666666; text-align: center;">
          <p style="margin: 0 0 8px 0; font-weight: bold; color: #333333;">С уважением, поддержка BrokenVK</p>
          <p style="margin: 0 0 4px 0;">Посетите наш официальный сайт: <a href="http://brokenvk.ru" style="color: #5181b8; text-decoration: none; font-weight: bold;">brokenvk.ru</a></p>
          <p style="margin: 0; font-size: 11px; color: #999999;">Зеркало проекта: <a href="http://brokenvk.ru" style="color: #777777; text-decoration: none;">brokenvk.ru</a></p>
        </div>
      </body>
    </html>
    """
    msg_alternative.attach(MIMEText(html_template, "html", "utf-8"))

    try:
        if os.path.exists(LOGO_PATH):
            with open(LOGO_PATH, "rb") as img_file:
                msg_image = MIMEImage(img_file.read(), _subtype="png")
                msg_image.add_header("Content-ID", "<project_logo>")
                msg_image.add_header("Content-Disposition", "inline", filename="logo.png")
                msg.attach(msg_image)
        else:
            _log(f"[WARN] Логотип не найден по пути: {LOGO_PATH}")
    except Exception as e:
        _log(f"Ошибка логотипа в автоответе: {e}")

    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_USER, user_email, msg.as_string())
    except Exception as e:
        _log(f"Ошибка SMTP автоответа: {e}")

# --- ОТПРАВКА ОТВЕТОВ МОДЕРАТОРОВ ПОЛЬЗОВАТЕЛЮ ---
def send_html_reply_smtp(user_email, moderator_text, moderator_nickname, ticket_number, attachment_bytes=None, attachment_name=None):
    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"Ответ по вашей заявке №{ticket_number}"
    msg["From"] = EMAIL_USER
    msg["To"] = user_email

    msg_related = MIMEMultipart("related")
    msg.attach(msg_related)
    msg_alternative = MIMEMultipart("alternative")
    msg_related.attach(msg_alternative)

    safe_text = (moderator_text or "").replace("\n", "<br>")

    html_template = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333333; line-height: 1.6; padding: 20px;">
        <div style="text-align: center; margin-bottom: 25px;">
          <img src="cid:project_logo" alt="BrokenVK" style="max-width: 220px; height: auto;" />
        </div>
        <div style="background-color: #f4f6f9; border-left: 4px solid #5181b8; padding: 15px; margin-bottom: 25px; border-radius: 4px;">
          {safe_text}
        </div>
        <hr style="border: 0; border-top: 1px solid #eeeeee; margin-bottom: 20px;">
        <div style="font-size: 13px; color: #666666;">
          <p style="margin: 0 0 5px 0; font-weight: bold;">С уважением, ваша поддержка BrokenVK</p>
          <p style="margin: 0; color: #5181b8;">Модератор: @{moderator_nickname} | Заявка №{ticket_number}</p>
        </div>
      </body>
    </html>
    """
    msg_alternative.attach(MIMEText(html_template, "html", "utf-8"))

    try:
        if os.path.exists(LOGO_PATH):
            with open(LOGO_PATH, "rb") as img_file:
                msg_image = MIMEImage(img_file.read(), _subtype="png")
                msg_image.add_header("Content-ID", "<project_logo>")
                msg_image.add_header("Content-Disposition", "inline", filename="logo.png")
                msg_related.attach(msg_image)
        else:
            _log(f"[WARN] Логотип не найден по пути: {LOGO_PATH}")
    except Exception as e:
        _log(f"Ошибка чтения логотипа: {e}")

    if attachment_bytes and attachment_name:
        content_type, _ = mimetypes.guess_type(attachment_name)
        if content_type is None:
            content_type = "application/octet-stream"
        main_type, sub_type = content_type.split("/", 1)
        part = MIMEBase(main_type, sub_type)
        part.set_payload(attachment_bytes)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename=("utf-8", "", attachment_name))
        msg.attach(part)

    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_USER, user_email, msg.as_string())
        return True
    except Exception as e:
        _log(f"Ошибка SMTP: {e}")
        return False

def extract_literal(data):
    """Достаёт тело RFC822 из ответа FETCH."""
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if not isinstance(data, (list, tuple)):
        return None

    parts = [bytes(item) for item in data if isinstance(item, (bytes, bytearray))]
    if not parts:
        return None

    for index, item in enumerate(parts):
        if item.rstrip().endswith(b"}") and index + 1 < len(parts):
            candidate = parts[index + 1]
            if candidate.strip():
                return candidate

    candidates = [item for item in parts if b"From:" in item or b"Return-Path:" in item or b"Subject:" in item]
    if candidates:
        return max(candidates, key=len)
    return None

async def fetch_raw_email(imap_client, num):
    """Тянет письмо через BODY.PEEK[], чтобы оно не помечалось прочитанным до обработки."""
    try:
        status, data = await imap_client.fetch(num, "(BODY.PEEK[])")
    except Exception as exc:
        return None, f"fetch exception: {exc}"
    if status != "OK":
        return None, f"status={status} data={data!r}"

    raw_email = extract_literal(data)
    if not raw_email:
        try:
            status_rfc, data_rfc = await imap_client.fetch(num, "(RFC822)")
            if status_rfc == "OK":
                raw_email = extract_literal(data_rfc)
        except Exception:
            pass

    return raw_email, data

# --- ПОСТОЯННЫЙ ОПРОС ПОЧТЫ (ВХОДЯЩИЕ И СПАМ) ---
_last_imap_error = None


def _log_once(msg):
    """Печатает сообщение, только если оно отличается от предыдущего."""
    global _last_imap_error
    if msg != _last_imap_error:
        _last_imap_error = msg
        _log(msg)


async def check_imap_email_loop():
    while True:
        try:
            imap_client = aioimaplib.IMAP4_SSL(host=IMAP_SERVER, port=IMAP_PORT)
            await imap_client.wait_hello_from_server()
            login_status, login_lines = await imap_client.login(EMAIL_USER, EMAIL_PASSWORD)
            if login_status != "OK":
                _log_once(
                    f"[IMAP] Логин отклонён сервером: {login_lines} — повтор через 5 минут"
                )
                try:
                    await imap_client.logout()
                except Exception:
                    pass
                await asyncio.sleep(300)
                continue
            _last_imap_error = None

            folders_to_check = ["INBOX"]
            for spam_cand in ['"&BCEEPwQw-"', '&BCEEPwQw-', 'Spam', 'Junk']:
                sel_status, _ = await imap_client.select(spam_cand)
                if sel_status == "OK":
                    folders_to_check.append(spam_cand)
                    break

            for folder in folders_to_check:
                sel_status, _ = await imap_client.select(folder)
                if sel_status != "OK":
                    continue

                status, data = await imap_client.search("UNSEEN")
                if status != "OK" or not data:
                    continue

                if isinstance(data, (list, tuple)):
                    search_text = b" ".join(
                        item if isinstance(item, bytes) else str(item).encode("ascii", errors="ignore")
                        for item in data
                    ).decode("ascii", errors="ignore")
                elif isinstance(data, bytes):
                    search_text = data.decode("ascii", errors="ignore")
                else:
                    search_text = str(data)

                # Фильтруем ТОЛЬКО числа, чтобы отсечь служебные слова 'SEARCH' и 'completed'
                msg_nums = [n for n in search_text.split() if n.isdigit()]

                for num in msg_nums:
                    raw_email, fetch_data = await fetch_raw_email(imap_client, num)
                    if raw_email is None:
                        _log_once(f"[IMAP] Не удалось извлечь письмо с ID {num} в {folder} | ответ сервера: {repr(fetch_data)[:300]}")
                        continue

                    msg = email.message_from_bytes(raw_email)

                    subject = decode_mime_header(msg.get("Subject", ""))
                    from_user = decode_mime_header(msg.get("From", ""))

                    body = ""
                    attachments = []

                    if msg.is_multipart():
                        for part in msg.walk():
                            content_type = part.get_content_type()
                            content_disposition = str(part.get("Content-Disposition"))

                            if content_type == "text/plain" and "attachment" not in content_disposition:
                                try:
                                    body = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                                except Exception:
                                    pass
                            elif "attachment" in content_disposition or part.get_filename():
                                filename = decode_mime_header(part.get_filename())
                                file_data = part.get_payload(decode=True)
                                if file_data:
                                    attachments.append({"filename": filename, "data": file_data, "type": content_type})
                    else:
                        payload = msg.get_payload(decode=True)
                        if payload:
                            body = payload.decode("utf-8", errors="ignore")

                    clean_email = parseaddr(from_user)[1]

                    # Письма из ЧС: сразу прочитано, без тикета, автоответа и уведомления
                    if is_blacklisted(clean_email):
                        _log(f"[ЧС] Письмо от {clean_email} проигнорировано")
                        await imap_client.store(num, "+FLAGS", "\\Seen")
                        continue

                    folder_tag = f" ({folder})" if folder != "INBOX" else ""
                    text_for_mods = (
                        f"📩 Новое обращение в саппорт!{folder_tag}\n\n"
                        f"Отправитель: {from_user}\n"
                        f"Тема: {subject}\n\n"
                        f"Текст сообщения:\n{body}"
                    )
                    sent_msg = await bot.send_message(chat_id=MODERATOR_CHAT_ID, text=text_for_mods)
                    main_msg_id = sent_msg.message_id

                    ticket_no = save_ticket(main_msg_id, clean_email)

                    await bot.send_message(chat_id=MODERATOR_CHAT_ID, text=f"🏷 Обращению присвоен №{ticket_no}", reply_to_message_id=main_msg_id)

                    loop = asyncio.get_running_loop()
                    loop.run_in_executor(None, send_auto_confirmation_email, clean_email, ticket_no)

                    if attachments:
                        for attach in attachments:
                            tg_file = types.BufferedInputFile(attach["data"], filename=attach["filename"])
                            ftype = attach["type"]
                            fname = attach["filename"]
                            try:
                                if "image" in ftype:
                                    await bot.send_photo(chat_id=MODERATOR_CHAT_ID, photo=tg_file, caption=f"🖼 Фото: {fname}", reply_to_message_id=main_msg_id)
                                elif "video" in ftype:
                                    await bot.send_video(chat_id=MODERATOR_CHAT_ID, video=tg_file, caption=f"📹 Видео: {fname}", reply_to_message_id=main_msg_id)
                                else:
                                    await bot.send_document(chat_id=MODERATOR_CHAT_ID, document=tg_file, caption=f"📁 Файл: {fname}", reply_to_message_id=main_msg_id)
                            except Exception as e:
                                _log(f"Ошибка отправки вложения модераторам: {e}")

                    # Помечаем прочитанным только после успешной обработки
                    await imap_client.store(num, "+FLAGS", "\\Seen")

            await imap_client.logout()

        except Exception as e:
            _log_once(f"[IMAP Ошибка]: {e}")

        await asyncio.sleep(60)

# --- КОМАНДЫ ЧЁРНОГО СПИСКА ---
BAN_USAGE = (
    "Как использовать:\n"
    "• /ban spam@example.com спам — по адресу\n"
    "• /ban спам — ответом на сообщение заявки, адрес возьмётся из базы\n"
    "• /unban spam@example.com — убрать из ЧС\n"
    "• /banlist — показать чёрный список"
)


def _is_mod_chat(message):
    return message.chat.id == MODERATOR_CHAT_ID


def _mod_name(message):
    user = message.from_user
    if user is None:
        return "неизвестный"
    if user.username:
        return f"@{user.username}"
    return user.full_name or str(user.id)


def _command_args(message):
    parts = (message.text or "").split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


def _ban_target(message):
    """(email, reason): адрес из аргументов, иначе из заявки, на которую ответили."""
    args = _command_args(message)
    email, reason = None, args

    if args:
        first, _, rest = args.partition(" ")
        if "@" in first:
            email, reason = first, rest.strip()

    if email is None and message.reply_to_message is not None:
        ticket_email, _, _ = get_ticket_info(message.reply_to_message.message_id)
        if ticket_email:
            email = ticket_email

    return (email.strip().strip("<>").lower() if email else None), reason


@dp.message(Command("ban"))
async def cmd_ban(message: types.Message):
    if not _is_mod_chat(message):
        return

    email, reason = _ban_target(message)
    if not email:
        await message.reply(f"❌ Не понял, кого банить.\n\n{BAN_USAGE}")
        return
    if not EMAIL_RE.match(email):
        await message.reply(f"❌ Это не похоже на e-mail: {email}")
        return

    is_new = add_to_blacklist(email, reason, _mod_name(message))
    title = "🚫 Добавлен в ЧС" if is_new else "♻️ Обновлён в ЧС"
    await message.reply(
        f"{title}: {email}\n"
        f"Причина: {reason or '—'}\n"
        f"Добавил: {_mod_name(message)}"
    )


@dp.message(Command("unban", "unblock"))
async def cmd_unban(message: types.Message):
    if not _is_mod_chat(message):
        return

    args = _command_args(message)
    email = args.split()[0].strip("<>").lower() if args else None

    if not email and message.reply_to_message is not None:
        ticket_email, _, _ = get_ticket_info(message.reply_to_message.message_id)
        email = ticket_email.strip().lower() if ticket_email else None

    if not email:
        await message.reply("Укажите адрес: /unban spam@example.com")
        return

    if remove_from_blacklist(email):
        await message.reply(f"✅ {email} убран из ЧС.")
    else:
        await message.reply(f"ℹ️ {email} в ЧС не найден.")


@dp.message(Command("banlist", "blacklist"))
async def cmd_banlist(message: types.Message):
    if not _is_mod_chat(message):
        return

    rows = list_blacklist()
    if not rows:
        await message.answer("🧾 Чёрный список пуст.")
        return

    lines = [f"🧾 Чёрный список — {len(rows)}:"]
    for index, (email, reason, added_by, added_at) in enumerate(rows, start=1):
        lines.append(
            f"{index}. {email} — {reason or 'без причины'}\n"
            f"   {added_by or '—'}, {added_at or '—'}"
        )
    await message.answer("\n".join(lines)[:4000])


# --- ОБРАБОТКА ОТВЕТОВ МОДЕРАТОРОВ ИЗ ТЕЛЕГРАМ ---
@dp.message()
async def handle_moderator_reply(message: types.Message):
    if message.chat.id == MODERATOR_CHAT_ID and message.reply_to_message:
        reply_id = message.reply_to_message.message_id
        user_email, ticket_no, is_replied = get_ticket_info(reply_id)

        if user_email and ticket_no:
            if is_replied == 1:
                await message.reply("⚠️ На этот тикет уже был отправлен ответ! Повторная отправка заблокирована.")
                return

            mod_name = message.from_user.username if message.from_user.username else message.from_user.first_name

            file_bytes = None
            file_name = None
            target_file_id = None

            # Поддержка всех типов вложений от модераторов
            if message.photo:
                target_file_id = message.photo[-1].file_id
                file_name = "photo.jpg"
            elif message.video:
                target_file_id = message.video.file_id
                file_name = message.video.file_name or "video.mp4"
            elif message.animation:
                target_file_id = message.animation.file_id
                file_name = message.animation.file_name or "animation.mp4"
            elif message.audio:
                target_file_id = message.audio.file_id
                file_name = message.audio.file_name or "audio.mp3"
            elif message.voice:
                target_file_id = message.voice.file_id
                file_name = "voice.ogg"
            elif message.video_note:
                target_file_id = message.video_note.file_id
                file_name = "video_note.mp4"
            elif message.document:
                target_file_id = message.document.file_id
                file_name = message.document.file_name or "document.dat"

            if target_file_id:
                try:
                    file_info = await bot.get_file(target_file_id)
                    downloaded = await bot.download_file(file_info.file_path)
                    file_bytes = downloaded.getvalue()
                except Exception as exc:
                    await message.reply(f"❌ Не удалось скачать файл (возможно, он больше 20 МБ): {exc}")
                    return

            moderator_text = message.caption if message.caption else (message.text if message.text else ("Файл от поддержки" if file_bytes else ""))

            loop = asyncio.get_running_loop()
            success = await loop.run_in_executor(
                None,
                send_html_reply_smtp,
                user_email,
                moderator_text,
                mod_name,
                ticket_no,
                file_bytes,
                file_name,
            )

            if success:
                mark_ticket_as_replied(reply_id)
                await message.reply(f"✅ Ответ по заявке №{ticket_no} успешно доставлен.")
            else:
                await message.reply("❌ Ошибка отправки через SMTP.")
        else:
            await message.reply("❌ Не удалось найти получателя или номер заявки в базе данных.")


# ============ АВТООЧИСТКА ЛОГОВ РАЗ В 30 ДНЕЙ ============
NOHUP_LOG_PATH = os.path.join(BASE_DIR, "nohup.out")
LOG_CLEAR_DAYS = 30
LOG_CLEAR_INTERVAL = 60 * 60
LOG_CLEAR_MARKER = os.path.join(BASE_DIR, ".log_cycle.txt")


def logs_need_clear():
    """True, если с последней очистки прошло 30 дней. Первый запуск только ставит метку."""
    now = datetime.datetime.now().timestamp()
    if not os.path.exists(LOG_CLEAR_MARKER):
        with open(LOG_CLEAR_MARKER, "w", encoding="utf-8") as f:
            f.write(str(now))
        return False
    with open(LOG_CLEAR_MARKER, "r", encoding="utf-8") as f:
        started = float((f.read().strip() or "0"))
    if started <= 0:
        with open(LOG_CLEAR_MARKER, "w", encoding="utf-8") as f:
            f.write(str(now))
        return False
    return (now - started) >= LOG_CLEAR_DAYS * 86400


def clear_logs():
    for path in (LOG_PATH, NOHUP_LOG_PATH):
        try:
            if os.path.exists(path):
                with open(path, "w", encoding="utf-8"):
                    pass
        except Exception as exc:
            print(f"[log] Не удалось очистить {path}: {exc}")
    with open(LOG_CLEAR_MARKER, "w", encoding="utf-8") as f:
        f.write(str(datetime.datetime.now().timestamp()))


async def log_cleanup_loop():
    while True:
        try:
            if logs_need_clear():
                clear_logs()
                _log(f"[Логи] log.txt и nohup.out очищены, следующая очистка через {LOG_CLEAR_DAYS} дн.")
        except Exception as exc:
            print(f"[Логи] Ошибка автоочистки: {exc}")
        await asyncio.sleep(LOG_CLEAR_INTERVAL)


async def main():
    import logging

    # Ошибки aiogram и всего, кроме почты, идут в nohup.out
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in root.handlers[:]:
        root.removeHandler(h)
    file_handler = logging.FileHandler(NOHUP_LOG_PATH, encoding="utf-8")
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    root.addHandler(console_handler)

    # Почтовые ошибки и события — в log.txt
    _log(f"=== Бот запущен === файл: {os.path.abspath(__file__)} | база: {DB_PATH} | лог: {LOG_PATH} | aiogram: {NOHUP_LOG_PATH}")

    init_db()
    asyncio.create_task(check_imap_email_loop())
    asyncio.create_task(db_cleanup_loop())
    asyncio.create_task(log_cleanup_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
