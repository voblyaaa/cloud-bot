import asyncio
import hashlib
import io
import json
import logging
import os
import secrets
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto, InputMediaVideo, KeyboardButton, Message, ReplyKeyboardMarkup,
)
from dotenv import load_dotenv
from aiogram.exceptions import TelegramBadRequest
from PIL import Image
import html
from aiogram.types import WebAppInfo

# ──────────────────────────────────────────── Config
load_dotenv()
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
DB_PATH: str = os.getenv("DB_PATH", "storage.db")
ADMIN_ID: int = int(os.getenv("ADMIN_ID", "0"))
WEBAPP_URL: str = os.getenv("WEBAPP_URL", "")


FOLDERS_PER_PAGE = 8
FILES_PER_PAGE = 8
GROUP_FILES_PER_PAGE = 20
MAX_FOLDERS_FREE = 30
MAX_FOLDERS_PREMIUM = 200
MAX_FILES_FREE = 200
MAX_FILES_PREMIUM = 2000
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX = 30
SHARE_LINK_DAYS = 30
REFERRAL_BONUS_FOLDERS = 10

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# ──────────────────────────────────────────── Premium Emoji
PE: dict[str, str] = {
    "home": "6042137469204303531", "back": "5960671702059848143", "next": "5895383238473421210",
    "cross": "5774077015388852135", "check": "5774022692642492953", "check2": "6041919344995209164",
    "warning": "6030563507299160824", "question": "6030848053177486888", "info": "6028435952299413210",
    "cancel": "6030757850274336631", "folder": "5805550320985578625", "folder2": "5805648413743651862",
    "folder_open": "6039630677182254664", "folder_star": "5805506958995758422", "doc": "6034969813032374911",
    "doc2": "6050643982646513651", "archive": "5766994197705921104", "box": "5884479287171485878",
    "paperclip": "6039451237743595514", "upload": "5963103826075456248", "download": "6039802767931871481",
    "upload2": "6039391666177541160", "move": "5893316448670978477", "copy": "6030657343744644592",
    "trash": "6039522349517115015", "pencil": "6039779802741739617", "write": "5920046907782074235",
    "write2": "5922693616953725714", "search": "6032850693348399258", "filter": "5888620056551625531",
    "image": "6035128606563241721", "image2": "6030466823290360017", "video": "5884252508603289902",
    "video2": "5886579539064132088", "audio": "5938473438468378529", "audio2": "6037364759811068375",
    "mic": "6030722571412967168", "mic2": "5933678317935791830", "camera": "6030506650522096180",
    "film": "5944777041709633960", "sticker": "6032882536235932111", "profile": "6032994772321309200",
    "profile2": "5893192487324880883", "people": "6032609071373226027", "people2": "6033125983572201397",
    "crown": "5805553606635559688", "gift": "6032644646587338669", "star": "6028338546736107668",
    "star2": "5767199127775481841", "lock": "6037249452824072506", "unlock": "6037496202990194718",
    "key": "5776227595708273495", "shield": "6030537007350944596", "link": "5769289093221454192",
    "link2": "6028171274939797252", "notify": "6039486778597970865", "notify_off": "6039569594157371705",
    "broadcast": "6021418126061605425", "tag": "5886285355279193209", "tag2": "5884050696679986441",
    "calendar": "5890937706803894250", "clock": "5891211339170326418", "recent": "5775896410780079073",
    "stats": "5936143551854285132", "globe": "5776233299424843260", "sparkle": "5890925363067886150",
    "lightning": "5884428842780594914", "settings": "5904258298764334001", "eye": "6037397706505195857",
    "backup": "5963087934696459905", "robot": "6030400221232501136", "plus": "6032924188828767321",
    "bubble": "6030784887093464891", "new": "5895669571058142797", "pin": "6043896193887506430",
    "share": "6039422865189638057", "map": "6042011682497106307", "trophy": "6037428784888549034",
}

def pe(name: str, fallback: str = "") -> str:
    eid = PE.get(name)
    return f'<tg-emoji emoji-id="{eid}">{fallback}</tg-emoji>' if eid else fallback

def pei(name: str) -> Optional[str]:
    return PE.get(name)

# ──────────────────────────────────────────── Safe Pydub Import
HAS_PYDUB = False
try:
    from pydub import AudioSegment
    HAS_PYDUB = True
except Exception as e:
    logger.warning(f"Pydub unavailable or incompatible: {e}. Audio conversion disabled.")


# ──────────────────────────────────────────── Reply Keyboards
async def reply_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    settings = await get_user_settings(user_id)
    webapp_url = os.getenv("WEBAPP_URL", "https://example.com") # ← Укажи свой URL из .env
    buttons = [
        [
            KeyboardButton(text="Мои папки", icon_custom_emoji_id=pei("folder")),
            KeyboardButton(text="Создать папку", icon_custom_emoji_id=pei("plus")),
        ],
        [
            KeyboardButton(text="Web Менеджер", web_app=WebAppInfo(url=webapp_url), icon_custom_emoji_id=pei("robot")),
            KeyboardButton(text="Профиль", icon_custom_emoji_id=pei("profile")),
        ],
    ]
    hidden = settings.get("hide_buttons", [])
    buttons = [row for row in buttons if row[0].text not in hidden]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, persistent=True)


def reply_cancel(back_text: str = "Отмена") -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=back_text, icon_custom_emoji_id=pei("cross"))]],
        resize_keyboard=True, one_time_keyboard=True
    )


# ──────────────────────────────────────────── States
class States(StatesGroup):
    creating_folder = State()
    renaming_folder = State()
    uploading = State()
    naming_file = State()
    adding_caption = State()
    searching = State()
    renaming_file = State()
    adding_tag = State()
    setting_password = State()
    entering_password = State()
    creating_subfolder = State()
    choosing_folder_quick = State()
    naming_group = State()
    renaming_group = State()  
    admin_broadcast = State()
    admin_grant_premium = State()
    admin_search_user = State()
    waiting_url = State()
    select_folder_for_url = State()
    creating_pack_quick = State()
    selecting_groups_merge = State()
    naming_merged_group = State()
    choosing_folder_quick_pack = State()
    selecting_files = State()
    converting_file = State()
    selecting_target_folder_mass = State()


# ──────────────────────────────────────────── Rate Limiting
_rate_buckets: dict[int, list[float]] = defaultdict(list)


def _check_rate(user_id: int) -> bool:
    now = time.monotonic()
    bucket = _rate_buckets[user_id]
    bucket[:] = [t for t in bucket if now - t < RATE_LIMIT_WINDOW]
    if len(bucket) >= RATE_LIMIT_MAX:
        return False
    bucket.append(now)
    return True

def kb_folder_select_mode(folder_id: int, files: list, page: int, total: int, selected_ids: set) -> InlineKeyboardMarkup:
    rows = []
    for f in files:
        fid = f[0]
        fname = f[3]
        short = fname[:22] + "…" if len(fname) > 22 else fname
        is_sel = fid in selected_ids
        icon = pei("check2") if is_sel else pei("cross")
        rows.append([InlineKeyboardButton(
            text=f"{'' if is_sel else ''} {short}",
            callback_data=f"sel_toggle:{fid}:{folder_id}",
            icon_custom_emoji_id=icon
        )])

    # Пагинация
    nav = []
    total_pages = pages_total(total, FILES_PER_PAGE)
    if page > 0: nav.append(InlineKeyboardButton(text="◁", callback_data=f"sel_page:{folder_id}:{page-1}", icon_custom_emoji_id=pei("back")))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page + 1 < total_pages: nav.append(InlineKeyboardButton(text="▷", callback_data=f"sel_page:{folder_id}:{page+1}", icon_custom_emoji_id=pei("next")))
    if len(nav) > 1: rows.append(nav)

    # Действия
    if selected_ids:
        rows.append([
            InlineKeyboardButton(text="Удалить выбранные", callback_data=f"mass_delete_confirm:{folder_id}", icon_custom_emoji_id=pei("trash")),
            InlineKeyboardButton(text="Переместить", callback_data=f"mass_move_start:{folder_id}", icon_custom_emoji_id=pei("move"))
        ])
    rows.append([
        InlineKeyboardButton(text="Отмена выделения", callback_data=f"sel_cancel:{folder_id}", icon_custom_emoji_id=pei("cross")),
        InlineKeyboardButton(text="Назад", callback_data=f"folder:{folder_id}:0", icon_custom_emoji_id=pei("back"))
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data.startswith("sel_mode_start:"))
async def cb_sel_mode_start(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split(":")[1])
    await state.set_state(States.selecting_files)
    await state.update_data(sel_folder_id=folder_id, sel_page=0, selected_files=set())
    files, total = await files_get(folder_id, callback.from_user.id, 0)
    await callback.message.edit_text(
        f"{pe('check2','✅')} <b>Режим выделения</b>\nНажимайте на файлы, чтобы выбрать их для действий.",
        reply_markup=kb_folder_select_mode(folder_id, files, 0, total, set())
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("sel_toggle:"), States.selecting_files)
async def cb_sel_toggle(callback: CallbackQuery, state: FSMContext):
    _, fid_s, folder_id_s = callback.data.split(":")
    fid, folder_id = int(fid_s), int(folder_id_s)
    data = await state.get_data()
    selected = data.get("selected_files", set())
    if fid in selected: selected.discard(fid)
    else: selected.add(fid)
    await state.update_data(selected_files=selected)
    page = data.get("sel_page", 0)
    files, total = await files_get(folder_id, callback.from_user.id, page)
    await callback.message.edit_reply_markup(reply_markup=kb_folder_select_mode(folder_id, files, page, total, selected))
    await callback.answer()

@dp.callback_query(F.data.startswith("sel_page:"), States.selecting_files)
async def cb_sel_page(callback: CallbackQuery, state: FSMContext):
    _, folder_id_s, page_s = callback.data.split(":")
    folder_id, page = int(folder_id_s), int(page_s)
    await state.update_data(sel_page=page)
    data = await state.get_data()
    files, total = await files_get(folder_id, callback.from_user.id, page)
    await callback.message.edit_reply_markup(reply_markup=kb_folder_select_mode(folder_id, files, page, total, data.get("selected_files", set())))
    await callback.answer()

@dp.callback_query(F.data.startswith("sel_cancel:"))
async def cb_sel_cancel(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split(":")[1])
    await state.clear()
    await _show_folder(callback, folder_id, 0, state=state)
    await callback.answer("Выделение отменено")

@dp.callback_query(F.data.startswith("mass_delete_confirm:"))
async def cb_mass_delete_confirm(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    count = len(data.get("selected_files", set()))
    await callback.message.edit_text(
        f"{pe('warning','❗️')} <b>Удалить {count} файлов?</b>\nДействие необратимо.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Да, удалить", callback_data=f"mass_delete_do:{folder_id}", icon_custom_emoji_id=pei("check")),
             InlineKeyboardButton(text="Отмена", callback_data=f"sel_cancel:{folder_id}", icon_custom_emoji_id=pei("cross"))]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("mass_delete_do:"))
async def cb_mass_delete_do(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    selected = data.get("selected_files", set())
    for fid in selected:
        await file_delete(fid, callback.from_user.id)
    await state.clear()
    await _show_folder(callback, folder_id, 0, state=state)
    await callback.answer(f"✅ Удалено {len(selected)} файлов")

@dp.callback_query(F.data.startswith("mass_move_start:"))
async def cb_mass_move_start(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split(":")[1])
    folders, _ = await folders_get(callback.from_user.id)
    rows = []
    for fid, name, *_ in folders:
        if fid == folder_id: continue
        rows.append([InlineKeyboardButton(text=name, callback_data=f"mass_move_do:{folder_id}:{fid}", icon_custom_emoji_id=pei("folder"))])
    rows.append([InlineKeyboardButton(text="Отмена", callback_data=f"sel_cancel:{folder_id}", icon_custom_emoji_id=pei("cross"))])
    await callback.message.edit_text(
        f"{pe('move','↔️')} <b>Переместить выбранные файлы</b>\nВыберите папку назначения:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("mass_move_do:"))
async def cb_mass_move_do(callback: CallbackQuery, state: FSMContext):
    _, old_id_s, new_id_s = callback.data.split(":")
    old_id, new_id = int(old_id_s), int(new_id_s)
    data = await state.get_data()
    selected = data.get("selected_files", set())
    for fid in selected:
        await file_move(fid, callback.from_user.id, new_id)
    await state.clear()
    await _show_folder(callback, old_id, 0, state=state)
    await callback.answer(f"✅ Перемещено {len(selected)} файлов")

@dp.callback_query(F.data.startswith("convert_start:"))
async def cb_convert_start(callback: CallbackQuery, state: FSMContext):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id, folder_id = int(file_db_id_s), int(folder_id_s)
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True); return

    ftype = f[2]
    rows = []
    if ftype in ("photo", "sticker"):
        rows.append([InlineKeyboardButton(text="В JPG", callback_data=f"convert_do:{file_db_id}:{folder_id}:jpg", icon_custom_emoji_id=pei("image"))])
        rows.append([InlineKeyboardButton(text="В PNG", callback_data=f"convert_do:{file_db_id}:{folder_id}:png", icon_custom_emoji_id=pei("image2"))])
    elif ftype in ("audio", "voice"):
        rows.append([InlineKeyboardButton(text="В MP3", callback_data=f"convert_do:{file_db_id}:{folder_id}:mp3", icon_custom_emoji_id=pei("audio"))])
    else:
        await callback.answer("Конвертация недоступна для этого типа файла", show_alert=True); return

    rows.append([InlineKeyboardButton(text="Отмена", callback_data=f"file:{file_db_id}:{folder_id}", icon_custom_emoji_id=pei("cross"))])
    await callback.message.edit_text(
        f"{pe('settings','⚙️')} <b>Конвертация файла</b>\n{file_emoji_pe(ftype)} <b>{f[3]}</b>\nВыберите целевой формат:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await callback.answer()

def _convert_bytes(file_bytes: bytes, src_type: str, target_fmt: str) -> bytes:
    """Синхронная функция для выполнения в отдельном потоке через asyncio.to_thread"""
    if src_type in ("photo", "sticker"):
        try:
            img = Image.open(io.BytesIO(file_bytes)).convert("RGB")
        except Exception:
            raise ValueError(
                "Не удалось распознать изображение. "
                "Возможно, это анимированный стикер (WEBM) или видео, которое нельзя конвертировать в JPG/PNG."
            )
        out = io.BytesIO()
        fmt_map = {"jpg": "JPEG", "jpeg": "JPEG", "png": "PNG", "webp": "WEBP", "bmp": "BMP", "gif": "GIF"}
        pil_format = fmt_map.get(target_fmt.lower(), target_fmt.upper())
        img.save(out, format=pil_format)
        return out.getvalue()
        
    elif src_type in ("audio", "voice"):
        if not HAS_PYDUB:
            raise ValueError("Конвертация аудио недоступна: библиотека pydub не установлена или несовместима.")
        try:
            seg = AudioSegment.from_file(io.BytesIO(file_bytes))
            out = io.BytesIO()
            seg.export(out, format=target_fmt)
            return out.getvalue()
        except Exception as e:
            raise ValueError(f"Ошибка конвертации аудио: {e}")
            
    raise ValueError("Неподдерживаемый тип файла для конвертации")

@dp.callback_query(F.data.startswith("convert_do:"))
async def cb_convert_do(callback: CallbackQuery, state: FSMContext):
    _, file_db_id_s, folder_id_s, target_fmt = callback.data.split(":")
    file_db_id, folder_id = int(file_db_id_s), int(folder_id_s)
    f = await file_get(file_db_id)
    if not f: await callback.answer("Файл не найден", show_alert=True); return

    await callback.answer("⏳ Конвертирую... Это может занять время.")
    try:
        file_io = io.BytesIO()
        await bot.download(f[1], destination=file_io)
        converted_bytes = await asyncio.to_thread(_convert_bytes, file_io.getvalue(), f[2], target_fmt)

        new_name = f"{f[3].rsplit('.', 1)[0]}.{target_fmt}"
        new_file = BufferedInputFile(converted_bytes, filename=new_name)
        sent_msg = await bot.send_document(callback.from_user.id, new_file)
        new_file_id = sent_msg.document.file_id
        new_size = len(converted_bytes)
        new_type = "audio" if target_fmt == "mp3" else "photo"

        await file_rename(file_db_id, callback.from_user.id, new_name)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE files SET file_id=?, file_type=?, file_size=? WHERE id=? AND user_id=?",
                             (new_file_id, new_type, new_size, file_db_id, callback.from_user.id))
            await db.commit()

        await callback.message.edit_text(
            f"{pe('check','✅')} <b>Конвертация завершена!</b>\n"
            f"{file_emoji_pe(new_type)} <b>{new_name}</b>\n"
            f"{pe('box','📦')} Новый размер: <b>{format_size(new_size)}</b>",
            reply_markup=kb_file(file_db_id, folder_id, bool(f[7]), await file_get_tags(file_db_id), f[9])
        )
    except Exception as e:
        logger.error(f"Conversion error: {e}")
        safe_error = html.escape(str(e))
        await callback.message.edit_text(
            f"{pe('cross','❌')} <b>Ошибка конвертации</b>\n<code>{safe_error}</code>"
        )

# ──────────────────────────────────────────── Database init
async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER PRIMARY KEY, theme TEXT DEFAULT 'light', language TEXT DEFAULT 'ru',
                hide_buttons TEXT DEFAULT '[]');
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY, username TEXT DEFAULT '', first_name TEXT DEFAULT '',
                is_premium INTEGER DEFAULT 0, referral_bonus INTEGER DEFAULT 0,
                referred_by INTEGER DEFAULT NULL, created_at TEXT DEFAULT (datetime('now')));
            CREATE TABLE IF NOT EXISTS folders (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
                parent_id INTEGER DEFAULT NULL, name TEXT NOT NULL, is_public INTEGER DEFAULT 0,
                share_token TEXT DEFAULT NULL, share_expires TEXT DEFAULT NULL,
                share_password TEXT DEFAULT NULL, view_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(user_id) REFERENCES users(user_id),
                FOREIGN KEY(parent_id) REFERENCES folders(id));
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
                folder_id INTEGER NOT NULL, file_id TEXT NOT NULL, file_type TEXT NOT NULL,
                file_name TEXT NOT NULL, caption TEXT DEFAULT '', file_size INTEGER DEFAULT 0,
                is_starred INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(user_id) REFERENCES users(user_id),
                FOREIGN KEY(folder_id) REFERENCES folders(id));
            CREATE TABLE IF NOT EXISTS file_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
                folder_id INTEGER NOT NULL, group_name TEXT DEFAULT 'Группа',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(user_id) REFERENCES users(user_id),
                FOREIGN KEY(folder_id) REFERENCES folders(id));
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);
            CREATE TABLE IF NOT EXISTS file_tags (
                file_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
                PRIMARY KEY(file_id, tag_id),
                FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,
                FOREIGN KEY(tag_id) REFERENCES tags(id) ON DELETE CASCADE);
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT, referrer_id INTEGER NOT NULL,
                referred_id INTEGER NOT NULL UNIQUE, created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(referrer_id) REFERENCES users(user_id),
                FOREIGN KEY(referred_id) REFERENCES users(user_id));
            CREATE INDEX IF NOT EXISTS idx_files_user ON files(user_id);
            CREATE INDEX IF NOT EXISTS idx_files_folder ON files(folder_id);
            CREATE INDEX IF NOT EXISTS idx_folders_user ON folders(user_id);
            CREATE INDEX IF NOT EXISTS idx_folders_parent ON folders(parent_id);
        """)
        try:
            await db.execute("ALTER TABLE files ADD COLUMN group_id INTEGER REFERENCES file_groups(id)")
        except aiosqlite.OperationalError:
            pass
        await db.commit()


async def get_user_settings(user_id: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT theme, language, hide_buttons FROM user_settings WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
    if not row:
        return {"theme": "light", "language": "ru", "hide_buttons": []}
    return {"theme": row[0], "language": row[1], "hide_buttons": json.loads(row[2])}


# ──────────────────────────────────────────── Users DB
async def user_upsert(user_id: int, username: str | None, first_name: str, referred_by: int | None = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, username, first_name, referred_by) VALUES (?,?,?,?)",
                         (user_id, username or "", first_name, referred_by))
        await db.execute("UPDATE users SET username=?, first_name=? WHERE user_id=?", (username or "", first_name, user_id))
        await db.commit()
        if referred_by and referred_by != user_id:
            await db.execute("INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?,?)", (referred_by, user_id))
            await db.execute("UPDATE users SET referral_bonus = referral_bonus + ? WHERE user_id = ?", (REFERRAL_BONUS_FOLDERS, referred_by))
            await db.commit()


async def user_get(user_id: int) -> tuple | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, username, first_name, is_premium, referral_bonus, referred_by FROM users WHERE user_id=?", (user_id,)) as cur:
            return await cur.fetchone()


async def user_max_folders(user_id: int) -> int:
    u = await user_get(user_id)
    if not u: return MAX_FOLDERS_FREE
    bonus = u[4] or 0
    base = MAX_FOLDERS_PREMIUM if u[3] else MAX_FOLDERS_FREE
    return base + bonus


async def user_max_files(user_id: int) -> int:
    u = await user_get(user_id)
    if not u: return MAX_FILES_FREE
    return MAX_FILES_PREMIUM if u[3] else MAX_FILES_FREE


async def user_stats(user_id: int) -> tuple[int, int, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM folders WHERE user_id=? AND parent_id IS NULL", (user_id,)) as cur:
            folders_cnt = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*), COALESCE(SUM(file_size),0) FROM files WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            files_cnt, total_size = row[0], row[1]
    return folders_cnt, files_cnt, total_size

async def folder_groups_info(folder_id: int, user_id: int) -> list[dict]:
    """Возвращает актуальные паки папки напрямую из БД (не зависит от пагинации файлов)"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT g.id, g.group_name, COUNT(f.id), COALESCE(SUM(f.file_size),0) "
            "FROM file_groups g LEFT JOIN files f ON f.group_id=g.id "
            "WHERE g.folder_id=? AND g.user_id=? GROUP BY g.id",
            (int(folder_id), int(user_id))
        ) as cur:
            rows = await cur.fetchall()
    return [
        {"group_id": r[0], "name": r[1] or "Группа", "count": r[2], "size_str": format_size(r[3])}
        for r in rows
    ]

# ──────────────────────────────────────────── Admin DB helpers (unchanged but using group_id not needed)
async def admin_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c: users = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM folders WHERE parent_id IS NULL") as c: folders = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*), COALESCE(SUM(file_size),0) FROM files") as c:
            r = await c.fetchone(); files, size = r[0], r[1]
        async with db.execute("SELECT COUNT(*) FROM users WHERE is_premium=1") as c: premium = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM referrals") as c: refs = (await c.fetchone())[0]
        async with db.execute("SELECT file_type, COUNT(*) as n FROM files GROUP BY file_type ORDER BY n DESC LIMIT 5") as c:
            top_types = await c.fetchall()
    return dict(users=users, folders=folders, files=files, size=size, premium=premium, refs=refs, top_types=top_types)


async def admin_get_all_users(page: int = 0, per_page: int = 10) -> tuple[list, int]:
    offset = page * per_page
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cur: total = (await cur.fetchone())[0]
        async with db.execute("SELECT user_id, username, first_name, is_premium, referral_bonus, created_at FROM users ORDER BY created_at DESC LIMIT ? OFFSET ?", (per_page, offset)) as cur:
            rows = await cur.fetchall()
    return rows, total


async def admin_search_users(query: str) -> list:
    like = f"%{query}%"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, username, first_name, is_premium, referral_bonus, created_at FROM users WHERE user_id LIKE ? OR username LIKE ? OR first_name LIKE ? ORDER BY created_at DESC LIMIT 20", (like, like, like)) as cur:
            return await cur.fetchall()


async def admin_get_user_details(user_id: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, username, first_name, is_premium, referral_bonus, referred_by, created_at FROM users WHERE user_id=?", (user_id,)) as cur:
            user = await cur.fetchone()
        if not user: return {}
        async with db.execute("SELECT COUNT(*) FROM folders WHERE user_id=? AND parent_id IS NULL", (user_id,)) as cur: folders_cnt = (await cur.fetchone())[0]
        async with db.execute("SELECT COUNT(*), COALESCE(SUM(file_size),0) FROM files WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone(); files_cnt, total_size = row[0], row[1]
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (user_id,)) as cur: refs_cnt = (await cur.fetchone())[0]
    return {"user_id": user[0], "username": user[1], "first_name": user[2], "is_premium": bool(user[3]), "referral_bonus": user[4], "referred_by": user[5], "created_at": user[6], "folders_count": folders_cnt, "files_count": files_cnt, "total_size": total_size, "refs_count": refs_cnt}


async def admin_get_recent_users(limit: int = 5) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, username, first_name, is_premium, created_at FROM users ORDER BY created_at DESC LIMIT ?", (limit,)) as cur:
            return await cur.fetchall()


async def admin_get_top_users(limit: int = 10) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT u.user_id, u.username, u.first_name, u.is_premium, COUNT(f.id) as files, COALESCE(SUM(f.file_size),0) as size FROM users u LEFT JOIN files f ON u.user_id=f.user_id GROUP BY u.user_id ORDER BY size DESC LIMIT ?", (limit,)) as cur:
            return await cur.fetchall()


async def admin_toggle_premium(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT is_premium FROM users WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
        if not row: return False
        new = 0 if row[0] else 1
        await db.execute("UPDATE users SET is_premium=? WHERE user_id=?", (new, user_id))
        await db.commit()
    return bool(new)


async def admin_set_premium(user_id: int, value: bool) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_premium=? WHERE user_id=?", (1 if value else 0, user_id))
        await db.commit()
    return True


async def admin_get_all_ids() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users") as cur:
            return [r[0] for r in await cur.fetchall()]


# ──────────────────────────────────────────── Folders DB
async def folders_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM folders WHERE user_id=? AND parent_id IS NULL", (user_id,)) as cur:
            return (await cur.fetchone())[0]


async def folders_get(user_id: int, page: int = 0, parent_id: int | None = None) -> tuple[list, int]:
    offset = page * FOLDERS_PER_PAGE
    pid_clause = "parent_id IS NULL" if parent_id is None else f"parent_id={parent_id}"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"SELECT COUNT(*) FROM folders WHERE user_id=? AND {pid_clause}", (user_id,)) as cur: total = (await cur.fetchone())[0]
        async with db.execute(f"SELECT id, name, is_public, share_token, created_at, view_count FROM folders WHERE user_id=? AND {pid_clause} ORDER BY created_at DESC LIMIT ? OFFSET ?", (user_id, FOLDERS_PER_PAGE, offset)) as cur:
            rows = await cur.fetchall()
    return rows, total


async def folder_get(folder_id: int) -> tuple | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, user_id, name, is_public, share_token, share_expires, share_password, view_count, parent_id FROM folders WHERE id=?", (folder_id,)) as cur:
            return await cur.fetchone()


async def folder_get_by_token(token: str) -> tuple | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, user_id, name, is_public, share_token, share_expires, share_password, view_count FROM folders WHERE share_token=? AND is_public=1", (token,)) as cur:
            return await cur.fetchone()


async def folder_create(user_id: int, name: str, parent_id: int | None = None) -> int:
    if await check_folder_name_duplicate(user_id, name, parent_id):
        raise ValueError("Папка с таким именем уже существует на этом уровне")
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("INSERT INTO folders (user_id, name, parent_id) VALUES (?,?,?)", (user_id, name, parent_id))
        await db.commit()
        return cur.lastrowid


async def folder_rename(folder_id: int, user_id: int, name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE folders SET name=? WHERE id=? AND user_id=?", (name, folder_id, user_id))
        await db.commit()


async def folder_toggle_public(folder_id: int, user_id: int, days: int = SHARE_LINK_DAYS) -> tuple[bool, str]:
    folder = await folder_get(folder_id)
    if not folder or folder[1] != user_id: return False, ""
    if folder[3]:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE folders SET is_public=0, share_token=NULL, share_expires=NULL WHERE id=? AND user_id=?", (folder_id, user_id))
            await db.commit()
        return False, ""
    token = folder[4] if folder[4] else secrets.token_urlsafe(12)
    expires = (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE folders SET is_public=1, share_token=?, share_expires=? WHERE id=? AND user_id=?", (token, expires, folder_id, user_id))
        await db.commit()
    return True, token


async def folder_set_password(folder_id: int, user_id: int, password: str | None):
    hashed = hashlib.sha256(password.encode()).hexdigest() if password else None
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE folders SET share_password=? WHERE id=? AND user_id=?", (hashed, folder_id, user_id))
        await db.commit()


async def folder_increment_views(folder_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE folders SET view_count=view_count+1 WHERE id=?", (folder_id,))
        await db.commit()


async def folder_delete(folder_id: int, user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id FROM folders WHERE parent_id=? AND user_id=?", (folder_id, user_id)) as cur:
            sub_ids = [r[0] for r in await cur.fetchall()]
        for sid in sub_ids:
            await db.execute("DELETE FROM files WHERE folder_id=? AND user_id=?", (sid, user_id))
            await db.execute("DELETE FROM folders WHERE id=? AND user_id=?", (sid, user_id))
        await db.execute("DELETE FROM files WHERE folder_id=? AND user_id=?", (folder_id, user_id))
        await db.execute("DELETE FROM folders WHERE id=? AND user_id=?", (folder_id, user_id))
        await db.commit()


async def subfolder_count(parent_id: int, user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM folders WHERE parent_id=? AND user_id=?", (parent_id, user_id)) as cur:
            return (await cur.fetchone())[0]


async def subfolders_get(parent_id: int, user_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, name, is_public FROM folders WHERE parent_id=? AND user_id=? ORDER BY created_at DESC", (parent_id, user_id)) as cur:
            return await cur.fetchall()


# ──────────────────────────────────────────── Files DB (now with group_id)
async def files_get(folder_id: int, user_id: int, page: int = 0, file_type: str | None = None, sort: str = "date", starred_only: bool = False) -> tuple[list, int]:
    offset = page * FILES_PER_PAGE
    conds = ["folder_id=?", "user_id=?"]
    params = [folder_id, user_id]
    if file_type:
        conds.append("file_type=?")
        params.append(file_type)
    if starred_only:
        conds.append("is_starred=1")
    where = " AND ".join(conds)
    order = {"date": "created_at DESC", "size": "file_size DESC", "name": "file_name ASC"}.get(sort, "created_at DESC")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"SELECT COUNT(*) FROM files WHERE {where}", params) as cur: total = (await cur.fetchone())[0]
        async with db.execute(f"SELECT id, file_id, file_type, file_name, file_size, created_at, is_starred, caption, group_id FROM files WHERE {where} ORDER BY {order} LIMIT ? OFFSET ?", [*params, FILES_PER_PAGE, offset]) as cur:
            rows = await cur.fetchall()
    return rows, total


async def files_get_public(folder_id: int, page: int = 0) -> tuple[list, int]:
    offset = page * FILES_PER_PAGE
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM files WHERE folder_id=?", (folder_id,)) as cur: total = (await cur.fetchone())[0]
        async with db.execute("SELECT id, file_id, file_type, file_name, file_size, created_at, is_starred, caption, group_id FROM files WHERE folder_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?", (folder_id, FILES_PER_PAGE, offset)) as cur:
            rows = await cur.fetchall()
    return rows, total


async def files_recent(user_id: int, limit: int = 15) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT f.id, f.file_id, f.file_type, f.file_name, f.file_size, fo.name, f.folder_id, f.is_starred, f.group_id FROM files f JOIN folders fo ON fo.id=f.folder_id WHERE f.user_id=? ORDER BY f.created_at DESC LIMIT ?", (user_id, limit)) as cur:
            return await cur.fetchall()


async def files_starred(user_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT f.id, f.file_id, f.file_type, f.file_name, f.file_size, fo.name, f.folder_id, f.group_id FROM files f JOIN folders fo ON fo.id=f.folder_id WHERE f.user_id=? AND f.is_starred=1 ORDER BY f.file_name ASC", (user_id,)) as cur:
            return await cur.fetchall()


async def files_search(user_id: int, query: str, page: int = 0, tag: str | None = None) -> tuple[list, int]:
    offset = page * FILES_PER_PAGE
    async with aiosqlite.connect(DB_PATH) as db:
        if tag:
            async with db.execute("SELECT COUNT(*) FROM files f JOIN file_tags ft ON ft.file_id=f.id JOIN tags t ON t.id=ft.tag_id WHERE f.user_id=? AND t.name=?", (user_id, tag)) as cur: total = (await cur.fetchone())[0]
            async with db.execute("SELECT f.id, f.file_id, f.file_type, f.file_name, f.file_size, fo.name, f.folder_id, f.group_id FROM files f JOIN file_tags ft ON ft.file_id=f.id JOIN tags t ON t.id=ft.tag_id JOIN folders fo ON fo.id=f.folder_id WHERE f.user_id=? AND t.name=? ORDER BY f.created_at DESC LIMIT ? OFFSET ?", (user_id, tag, FILES_PER_PAGE, offset)) as cur:
                rows = await cur.fetchall()
        else:
            like = f"%{query}%"
            async with db.execute("SELECT COUNT(*) FROM files WHERE user_id=? AND file_name LIKE ?", (user_id, like)) as cur: total = (await cur.fetchone())[0]
            async with db.execute("SELECT f.id, f.file_id, f.file_type, f.file_name, f.file_size, fo.name, f.folder_id, f.group_id FROM files f JOIN folders fo ON fo.id=f.folder_id WHERE f.user_id=? AND f.file_name LIKE ? ORDER BY f.created_at DESC LIMIT ? OFFSET ?", (user_id, like, FILES_PER_PAGE, offset)) as cur:
                rows = await cur.fetchall()
    return rows, total


async def file_get(file_db_id: int) -> tuple | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, file_id, file_type, file_name, file_size, folder_id, user_id, is_starred, caption, group_id FROM files WHERE id=?", (file_db_id,)) as cur:
            return await cur.fetchone()


async def file_save(user_id: int, folder_id: int, file_id: str, file_type: str, file_name: str, file_size: int, caption: str = "", group_id: int | None = None) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("INSERT INTO files (user_id, folder_id, file_id, file_type, file_name, file_size, caption, group_id) VALUES (?,?,?,?,?,?,?,?)",
                               (user_id, folder_id, file_id, file_type, file_name, file_size, caption, group_id))
        await db.commit()
        return cur.lastrowid


async def file_move(file_db_id: int, user_id: int, new_folder_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE files SET folder_id=? WHERE id=? AND user_id=?", (new_folder_id, file_db_id, user_id))
        await db.commit()


async def file_copy(file_db_id: int, user_id: int, new_folder_id: int) -> int:
    f = await file_get(file_db_id)
    if not f: raise ValueError("file not found")
    # pass group_id from original file
    return await file_save(user_id, new_folder_id, f[1], f[2], f[3], f[4], f[8] or "", f[9] if len(f) > 9 else None)


async def file_delete(file_db_id: int, user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM files WHERE id=? AND user_id=?", (file_db_id, user_id))
        await db.commit()


async def file_rename(file_db_id: int, user_id: int, new_name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE files SET file_name=? WHERE id=? AND user_id=?", (new_name, file_db_id, user_id))
        await db.commit()


async def file_toggle_star(file_db_id: int, user_id: int) -> bool:
    f = await file_get(file_db_id)
    if not f: return False
    new = 0 if f[7] else 1
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE files SET is_starred=? WHERE id=? AND user_id=?", (new, file_db_id, user_id))
        await db.commit()
    return bool(new)


async def file_set_caption(file_db_id: int, user_id: int, caption: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE files SET caption=? WHERE id=? AND user_id=?", (caption, file_db_id, user_id))
        await db.commit()


# ──────────────────────────────────────────── Group management (NEW)

async def group_merge(user_id: int, folder_id: int, source_group_ids: list[int], new_group_name: str, target_group_id: int | None = None) -> int:
    source_group_ids = [int(gid) for gid in source_group_ids]
    if len(source_group_ids) < 2:
        raise ValueError("Нужно минимум два пака для объединения")
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE")
        try:
            placeholders = ",".join("?" for _ in source_group_ids)
            async with db.execute(
                f"SELECT id, folder_id FROM file_groups WHERE id IN ({placeholders}) AND user_id=?",
                [*source_group_ids, int(user_id)]
            ) as cur:
                rows = await cur.fetchall()
            if len(rows) != len(source_group_ids):
                raise ValueError("Некоторые паки не найдены или вам не принадлежат")
            if any(r[1] != folder_id for r in rows):
                raise ValueError("Все паки должны находиться в одной папке")
            
            if target_group_id:
                new_gid = int(target_group_id)
                if new_gid not in source_group_ids:
                    raise ValueError("Целевой пак должен быть среди выбранных")
            else:
                cur = await db.execute(
                    "INSERT INTO file_groups (user_id, folder_id, group_name) VALUES (?,?,?)",
                    (int(user_id), int(folder_id), new_group_name)
                )
                new_gid = cur.lastrowid
            
            # Переносим все файлы в новый пак
            await db.execute(
                f"UPDATE files SET group_id=? WHERE group_id IN ({placeholders}) AND user_id=?",
                [new_gid, *source_group_ids, int(user_id)]
            )
            
            # Удаляем старые записи паков
            to_delete = [gid for gid in source_group_ids if gid != new_gid]
            if to_delete:
                ph_del = ",".join("?" for _ in to_delete)
                await db.execute(
                    f"DELETE FROM file_groups WHERE id IN ({ph_del}) AND user_id=?",
                    [*to_delete, int(user_id)]
                )
            
            await db.commit()
            return new_gid
        except Exception as e:
            await db.rollback()
            logger.error(f"Group merge failed: {e}")
            raise e

async def group_get(group_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, user_id, folder_id, group_name, created_at FROM file_groups WHERE id=?",
            (group_id,)
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return None
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*), COALESCE(SUM(file_size),0) FROM files WHERE group_id=?",
            (group_id,)
        ) as cur:
            cnt, size = await cur.fetchone()
    return {
        "group_id": row[0],
        "user_id": row[1],
        "folder_id": row[2],
        "group_name": row[3],
        "created_at": row[4],
        "files_count": cnt,
        "total_size": size,
    }


async def group_rename(group_id: int, user_id: int, new_name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE file_groups SET group_name=? WHERE id=? AND user_id=?",
            (new_name, group_id, user_id)
        )
        await db.commit()


async def group_files_list(group_id: int, page: int | None = 0) -> tuple[list, int]:
    """Возвращает (файлы, общее_кол-во). Если page=None, возвращает все файлы без пагинации."""
    async with aiosqlite.connect(DB_PATH) as db:
        if page is None:
            async with db.execute("SELECT id, file_id, file_type, file_name, file_size, is_starred, caption FROM files WHERE group_id=? ORDER BY created_at ASC", (group_id,)) as cur:
                rows = await cur.fetchall()
            return rows, len(rows)
        offset = page * GROUP_FILES_PER_PAGE
        async with db.execute("SELECT COUNT(*) FROM files WHERE group_id=?", (group_id,)) as cur:
            total = (await cur.fetchone())[0]
        async with db.execute("SELECT id, file_id, file_type, file_name, file_size, is_starred, caption FROM files WHERE group_id=? ORDER BY created_at ASC LIMIT ? OFFSET ?", (group_id, GROUP_FILES_PER_PAGE, offset)) as cur:
            rows = await cur.fetchall()
        return rows, total


async def group_remove_file(file_db_id: int, user_id: int):
    """Remove a file from its group, keeping the file itself."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE files SET group_id=NULL WHERE id=? AND user_id=?",
            (file_db_id, user_id)
        )
        await db.commit()


async def group_delete(group_id: int, user_id: int):
    """Delete the group record; files remain in folder but become ungrouped."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE files SET group_id=NULL WHERE group_id=? AND user_id=?",
            (group_id, user_id)
        )
        await db.execute(
            "DELETE FROM file_groups WHERE id=? AND user_id=?",
            (group_id, user_id)
        )
        await db.commit()


async def folder_groups_list(folder_id: int, user_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, group_name FROM file_groups WHERE folder_id=? AND user_id=? ORDER BY group_name",
            (folder_id, user_id)
        ) as cur:
            return await cur.fetchall()


# ──────────────────────────────────────────── Tags
async def tag_get_or_create(name: str) -> int:
    name = name.strip().lower().lstrip("#")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id FROM tags WHERE name=?", (name,)) as cur:
            row = await cur.fetchone()
        if row: return row[0]
        cur2 = await db.execute("INSERT INTO tags (name) VALUES (?)", (name,))
        await db.commit()
        return cur2.lastrowid


async def file_add_tag(file_db_id: int, tag_name: str):
    tid = await tag_get_or_create(tag_name)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO file_tags (file_id, tag_id) VALUES (?,?)", (file_db_id, tid))
        await db.commit()


async def file_get_tags(file_db_id: int) -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT t.name FROM tags t JOIN file_tags ft ON ft.tag_id=t.id WHERE ft.file_id=?", (file_db_id,)) as cur:
            return [r[0] for r in await cur.fetchall()]


async def file_remove_tag(file_db_id: int, tag_name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id FROM tags WHERE name=?", (tag_name,)) as cur:
            row = await cur.fetchone()
        if row:
            await db.execute("DELETE FROM file_tags WHERE file_id=? AND tag_id=?", (file_db_id, row[0]))
            await db.commit()


async def user_all_tags(user_id: int) -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT DISTINCT t.name FROM tags t JOIN file_tags ft ON ft.tag_id=t.id JOIN files f ON f.id=ft.file_id WHERE f.user_id=? ORDER BY t.name", (user_id,)) as cur:
            return [r[0] for r in await cur.fetchall()]


# ──────────────────────────────────────────── Helpers

async def check_folder_cycle(user_id: int, parent_id: int | None, target_folder_id: int | None = None) -> bool:
    """Возвращает True, если создание/перемещение создаст цикл."""
    if parent_id is None:
        return False
    current = parent_id
    visited = set()
    while current:
        if current == target_folder_id:
            return True
        if current in visited:
            return False
        visited.add(current)
        f = await folder_get(current)
        if not f or f[1] != user_id:
            break
        current = f[8]  # parent_id
    return False

async def check_folder_name_duplicate(user_id: int, name: str, parent_id: int | None) -> bool:
    """Проверяет, есть ли папка с таким именем в том же уровне."""
    async with aiosqlite.connect(DB_PATH) as db:
        if parent_id is None:
            async with db.execute("SELECT 1 FROM folders WHERE user_id=? AND parent_id IS NULL AND name=? LIMIT 1", (user_id, name)) as cur:
                return await cur.fetchone() is not None
        else:
            async with db.execute("SELECT 1 FROM folders WHERE user_id=? AND parent_id=? AND name=? LIMIT 1", (user_id, parent_id, name)) as cur:
                return await cur.fetchone() is not None

async def get_folder_breadcrumbs(folder_id: int, user_id: int) -> str:
    """Возвращает путь вида: Главная / Работа / Проекты"""
    path = []
    current = folder_id
    for _ in range(20):  # защита от бесконечного цикла
        f = await folder_get(current)
        if not f or f[1] != user_id:
            break
        path.append(f[2])
        if f[8] is None:
            break
        current = f[8]
    return " / ".join(reversed(path)) if path else "Главная"

FILE_TYPE_INFO = {
    "photo": ("🖼", "Фото", "image"), "video": ("🎥", "Видео", "video"), "audio": ("🎶", "Аудио", "audio"),
    "voice": ("🎤", "Голосовое", "mic"), "document": ("📄", "Документ", "doc"), "animation": ("🎞", "Анимация", "film"),
    "video_note": ("📹", "Видеокружок", "camera"), "sticker": ("🎭", "Стикер", "sticker"),
}


def file_emoji_pe(file_type: str) -> str:
    info = FILE_TYPE_INFO.get(file_type, ("📄", "Файл", "doc"))
    return pe(info[2], info[0])


def file_emoji(file_type: str) -> str:
    return FILE_TYPE_INFO.get(file_type, ("📄", "Файл", "doc"))[0]


def file_type_name(file_type: str) -> str:
    return FILE_TYPE_INFO.get(file_type, ("📄", "Файл", "doc"))[1]


def file_pe_key(file_type: str) -> str:
    return FILE_TYPE_INFO.get(file_type, ("📄", "Файл", "doc"))[2]


def format_size(size: int) -> str:
    if size < 1024: return f"{size} B"
    elif size < 1024**2: return f"{size/1024:.1f} KB"
    elif size < 1024**3: return f"{size/1024**2:.1f} MB"
    else: return f"{size/1024**3:.2f} GB"


def pages_total(total: int, per_page: int) -> int:
    return max(1, (total + per_page - 1) // per_page)


def _is_expired(expires_str: str | None) -> bool:
    if not expires_str: return False
    try:
        exp = datetime.strptime(expires_str, "%Y-%m-%d %H:%M:%S")
        return datetime.now(timezone.utc()) > exp
    except: return False


def _progress_bar(current: int, total: int, width: int = 10) -> str:
    if total == 0: return "░" * width
    filled = int(width * current / total)
    return "█" * filled + "░" * (width - filled)


# ──────────────────────────────────────────── Keyboards
def kb_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Папки", callback_data="my_folders:0", icon_custom_emoji_id=pei("folder")),
         InlineKeyboardButton(text="Создать", callback_data="create_folder", icon_custom_emoji_id=pei("plus"))],
        [InlineKeyboardButton(text="Загрузить", callback_data="upload:", icon_custom_emoji_id=pei("upload")),
         InlineKeyboardButton(text="Поиск", callback_data="search_start", icon_custom_emoji_id=pei("search"))],
        [InlineKeyboardButton(text="Недавние", callback_data="recent_files", icon_custom_emoji_id=pei("recent")),
         InlineKeyboardButton(text="Избранное", callback_data="starred_files", icon_custom_emoji_id=pei("star"))],
        [InlineKeyboardButton(text="Профиль", callback_data="profile", icon_custom_emoji_id=pei("profile")),
         InlineKeyboardButton(text="Помощь", callback_data="help", icon_custom_emoji_id=pei("info"))],
    ])

def kb_folders(folders: list, page: int, total: int) -> InlineKeyboardMarkup:
    rows = []
    for fid, name, is_public, _, _, views in folders:
        icon = pei("unlock") if is_public else pei("folder")
        rows.append([InlineKeyboardButton(text=f"{name}", callback_data=f"folder:{fid}:0", icon_custom_emoji_id=icon)])
    nav = []
    total_pages = pages_total(total, FOLDERS_PER_PAGE)
    if page > 0: nav.append(InlineKeyboardButton(text="◁", callback_data=f"my_folders:{page-1}", icon_custom_emoji_id=pei("back")))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page + 1 < total_pages: nav.append(InlineKeyboardButton(text="▷", callback_data=f"my_folders:{page+1}", icon_custom_emoji_id=pei("next")))
    if len(nav) > 1: rows.append(nav)
    rows.append([InlineKeyboardButton(text="Новая папка", callback_data="create_folder", icon_custom_emoji_id=pei("plus"))])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="main_menu", icon_custom_emoji_id=pei("back"))])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_folder(
    folder_id: int,
    files: list,
    page: int,
    total: int,
    is_public: bool,
    sort: str = "date",
    file_type: str | None = None,
    subfolders: list | None = None,
    groups_info: list | None = None  # <-- НОВЫЙ ПАРАМЕТР
) -> InlineKeyboardMarkup:
    rows = []

    # Подпапки
    if subfolders:
        for sfid, sfname, sfpub in subfolders:
            rows.append([
                InlineKeyboardButton(
                    text=f"{sfname}",
                    callback_data=f"folder:{sfid}:0",
                    icon_custom_emoji_id=pei("folder_open")
                )
            ])

    # Паки (группы)
    if groups_info:
        for g in groups_info:
            rows.append([
                InlineKeyboardButton(
                    text=f"{g['name']} ({g['count']} шт., {g['size_str']})",
                    callback_data=f"group_view:{g['group_id']}",
                    icon_custom_emoji_id=pei("archive")
                )
            ])
        # --- Кнопка объединить паки ---
        if len(groups_info) >= 2:
            rows.append([
                InlineKeyboardButton(
                    text="Объединить паки",
                    callback_data=f"merge_groups_start:{folder_id}",
                    icon_custom_emoji_id=pei("archive")
                )
            ])

    # Файлы (те, у которых group_id == NULL)
    for f in files:
        group_id = f[8] if len(f) > 8 else None
        if group_id is not None:
            continue  # файлы в паках не показываем отдельно
        fid, _, ftype, fname, fsize, _, is_starred, caption = f[:8]
        star = "⭐️  " if is_starred else ""
        short = fname[:25] + "…" if len(fname) > 25 else fname
        rows.append([
            InlineKeyboardButton(
                text=f"{star}{short}",
                callback_data=f"file:{fid}:{folder_id}",
                icon_custom_emoji_id=pei(file_pe_key(ftype))
            )
        ])

    # Пагинация
    nav = []
    total_pages = pages_total(total, FILES_PER_PAGE)
    if page > 0:
        nav.append(InlineKeyboardButton(
            text="◁",
            callback_data=f"folder:{folder_id}:{page-1}",
            icon_custom_emoji_id=pei("back")
        ))
    nav.append(InlineKeyboardButton(
        text=f"{page+1}/{total_pages}",
        callback_data="noop"
    ))
    if page + 1 < total_pages:
        nav.append(InlineKeyboardButton(
            text="▷",
            callback_data=f"folder:{folder_id}:{page+1}",
            icon_custom_emoji_id=pei("next")
        ))
    if len(nav) > 1:
        rows.append(nav)

    # Основные кнопки
    rows.append([
        InlineKeyboardButton(text="Загрузить", callback_data=f"upload:{folder_id}", icon_custom_emoji_id=pei("upload")),
        InlineKeyboardButton(text="Подпапка", callback_data=f"new_subfolder:{folder_id}", icon_custom_emoji_id=pei("folder_open")),
        InlineKeyboardButton(text="Выделить", callback_data=f"sel_mode_start:{folder_id}", icon_custom_emoji_id=pei("check2"))
    ])
    rows.append([
        InlineKeyboardButton(text="Поиск", callback_data="search_start", icon_custom_emoji_id=pei("search")),
        InlineKeyboardButton(text="Действия", callback_data=f"folder_actions:{folder_id}", icon_custom_emoji_id=pei("settings")),
        InlineKeyboardButton(text="Фильтр/Сорт", callback_data=f"folder_filter:{folder_id}", icon_custom_emoji_id=pei("filter"))
    ])
    if WEBAPP_URL:
        rows.append([
            InlineKeyboardButton(
                text="📱 Web Менеджер",
                web_app=WebAppInfo(url=f"{WEBAPP_URL}/?folder_id={folder_id}"),
                icon_custom_emoji_id=pei("robot")
            ),
        ])
    if is_public:
        rows.append([
            InlineKeyboardButton(
                text="Ссылка",
                callback_data=f"share_link:{folder_id}",
                icon_custom_emoji_id=pei("link")
            )
        ])
    rows.append([
        InlineKeyboardButton(
            text="Назад",
            callback_data="my_folders:0",
            icon_custom_emoji_id=pei("back")
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_folder_actions(folder_id: int, is_public: bool) -> InlineKeyboardMarkup:
    open_close_text = "Закрыть папку" if is_public else "Открыть папку"
    open_close_icon = pei("lock") if is_public else pei("unlock")
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Переименовать", callback_data=f"rename:{folder_id}", icon_custom_emoji_id=pei("pencil")),
         InlineKeyboardButton(text="Удалить", callback_data=f"delfolder:{folder_id}", icon_custom_emoji_id=pei("trash"))],
        [InlineKeyboardButton(text=open_close_text, callback_data=f"toggle_public:{folder_id}", icon_custom_emoji_id=open_close_icon),
         InlineKeyboardButton(text="Пароль", callback_data=f"set_password:{folder_id}", icon_custom_emoji_id=pei("key"))],
        [InlineKeyboardButton(text="Бэкап", callback_data=f"backup_folder:{folder_id}", icon_custom_emoji_id=pei("backup")),
         InlineKeyboardButton(text="Скачать ZIP", callback_data=f"export_zip:{folder_id}", icon_custom_emoji_id=pei("archive"))],
        [InlineKeyboardButton(text="Просмотры", callback_data=f"folder_views:{folder_id}", icon_custom_emoji_id=pei("eye"))],
        [InlineKeyboardButton(text="Назад", callback_data=f"folder:{folder_id}:0", icon_custom_emoji_id=pei("back"))]
    ])

def kb_folder_filter_sort(folder_id: int, current_sort: str, current_filter: str | None) -> InlineKeyboardMarkup:
    rows = []
    
    # Строка сортировки
    sort_row = []
    # Дата
    sort_row.append(InlineKeyboardButton(
        text="Дата" if current_sort != "date" else f"[Дата]",
        callback_data=f"sort:{folder_id}:date",
        icon_custom_emoji_id=pei("calendar")
    ))
    # Размер
    sort_row.append(InlineKeyboardButton(
        text="Размер" if current_sort != "size" else f"[Размер]",
        callback_data=f"sort:{folder_id}:size",
        icon_custom_emoji_id=pei("box")
    ))
    # Имя
    sort_row.append(InlineKeyboardButton(
        text="Имя" if current_sort != "name" else f"[Имя]",
        callback_data=f"sort:{folder_id}:name",
        icon_custom_emoji_id=pei("write")
    ))
    rows.append(sort_row)
    
    # Строки фильтра
    filter_row1 = []
    filter_row2 = []
    
    # Все
    filter_row1.append(InlineKeyboardButton(
        text="Все" if current_filter is None else f"[Все]",
        callback_data=f"filter:{folder_id}:all",
        icon_custom_emoji_id=pei("filter")
    ))
    # Документы
    filter_row1.append(InlineKeyboardButton(
        text="Документы" if current_filter != "document" else f"[Документы]",
        callback_data=f"filter:{folder_id}:document",
        icon_custom_emoji_id=pei("doc")
    ))
    # Фото
    filter_row1.append(InlineKeyboardButton(
        text="Фото" if current_filter != "photo" else f"[Фото]",
        callback_data=f"filter:{folder_id}:photo",
        icon_custom_emoji_id=pei("image")
    ))
    # Видео
    filter_row2.append(InlineKeyboardButton(
        text="Видео" if current_filter != "video" else f"[Видео]",
        callback_data=f"filter:{folder_id}:video",
        icon_custom_emoji_id=pei("video")
    ))
    # Аудио
    filter_row2.append(InlineKeyboardButton(
        text="Аудио" if current_filter != "audio" else f"[Аудио]",
        callback_data=f"filter:{folder_id}:audio",
        icon_custom_emoji_id=pei("audio")
    ))
    rows.append(filter_row1)
    rows.append(filter_row2)
    
    # Кнопка "Применить"
    rows.append([InlineKeyboardButton(
        text="Применить",
        callback_data=f"folder:{folder_id}:0",
        icon_custom_emoji_id=pei("check")
    )])
    # Кнопка "Назад"
    rows.append([InlineKeyboardButton(
        text="Назад",
        callback_data=f"folder:{folder_id}:0",
        icon_custom_emoji_id=pei("back")
    )])
    
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data.startswith("folder_actions:"))
async def cb_folder_actions(callback: CallbackQuery):
    folder_id = int(callback.data.split(":")[1])
    folder = await folder_get(folder_id)
    if not folder or folder[1] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return
    is_public = bool(folder[3])
    await callback.message.edit_reply_markup(reply_markup=kb_folder_actions(folder_id, is_public))
    await callback.answer()

@dp.callback_query(F.data.startswith("folder_filter:"))
async def cb_folder_filter(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    current_sort = data.get("current_sort", "date")
    current_filter = data.get("current_filter")
    await callback.message.edit_reply_markup(reply_markup=kb_folder_filter_sort(folder_id, current_sort, current_filter))
    await callback.answer()

@dp.callback_query(F.data.startswith("sort:"))
async def cb_sort_from_filter(callback: CallbackQuery, state: FSMContext):
    _, folder_id, sort_key = callback.data.split(":")
    await _show_folder(callback, int(folder_id), 0, sort=sort_key, state=state)
    await callback.answer()

@dp.callback_query(F.data.startswith("filter:"))
async def cb_filter_from_filter(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    folder_id = int(parts[1])
    flt_key = parts[2]
    type_map = {"all": None, "document": "document", "photo": "photo", "video": "video", "audio": "audio"}
    await _show_folder(callback, folder_id, 0, file_type=type_map.get(flt_key), state=state)
    await callback.answer()

def kb_public_folder(folder_id: int, files: list, page: int, total: int) -> InlineKeyboardMarkup:
    rows = []
    for fid, _, ftype, fname, fsize, _, is_starred, caption, group_id in files:
        if group_id: continue
        short = fname[:27] + "…" if len(fname) > 27 else fname
        rows.append([InlineKeyboardButton(text=short, callback_data=f"pub_dl:{fid}", icon_custom_emoji_id=pei(file_pe_key(ftype)))])
    nav = []
    total_pages = pages_total(total, FILES_PER_PAGE)
    if page > 0: nav.append(InlineKeyboardButton(text="◁", callback_data=f"pub_folder:{folder_id}:{page-1}", icon_custom_emoji_id=pei("back")))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page + 1 < total_pages: nav.append(InlineKeyboardButton(text="▷", callback_data=f"pub_folder:{folder_id}:{page+1}", icon_custom_emoji_id=pei("next")))
    if len(nav) > 1: rows.append(nav)
    rows.append([InlineKeyboardButton(text="Главная", callback_data="main_menu", icon_custom_emoji_id=pei("home"))])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_file(file_db_id: int, folder_id: int, is_starred: bool = False, tags: list | None = None, group_id: int | None = None) -> InlineKeyboardMarkup:
    star_text = "Убрать из избранного" if is_starred else "В избранное"
    rows = [
        [InlineKeyboardButton(text="Скачать", callback_data=f"download:{file_db_id}:{folder_id}", icon_custom_emoji_id=pei("download")),
         InlineKeyboardButton(text="Конвертировать", callback_data=f"convert_start:{file_db_id}:{folder_id}", icon_custom_emoji_id=pei("settings")),
         InlineKeyboardButton(text="Переместить", callback_data=f"move_start:{file_db_id}:{folder_id}", icon_custom_emoji_id=pei("move"))],
        [InlineKeyboardButton(text=star_text, callback_data=f"star:{file_db_id}:{folder_id}", icon_custom_emoji_id=pei("star"))],
        [InlineKeyboardButton(text="Переименовать", callback_data=f"renamefile:{file_db_id}:{folder_id}", icon_custom_emoji_id=pei("pencil")),
         InlineKeyboardButton(text="Описание", callback_data=f"editcaption:{file_db_id}:{folder_id}", icon_custom_emoji_id=pei("write"))],
        [InlineKeyboardButton(text="Добавить тег", callback_data=f"addtag:{file_db_id}:{folder_id}", icon_custom_emoji_id=pei("tag"))],
    ]
    if group_id is not None:
        rows.append([
            InlineKeyboardButton(text="Переместить в другой пак", callback_data=f"move_to_pack_start:{file_db_id}:{folder_id}",
                                 icon_custom_emoji_id=pei("move")),
            InlineKeyboardButton(text="Убрать из пака", callback_data=f"remove_from_group:{file_db_id}:{folder_id}",
                                 icon_custom_emoji_id=pei("cross"))
        ])
    else:
        rows.append([InlineKeyboardButton(text="Добавить в пак", callback_data=f"add_to_group:{file_db_id}:{folder_id}",
                                          icon_custom_emoji_id=pei("plus"))])
    if tags:
        for tg in tags:
            rows.append([InlineKeyboardButton(text=f"❌  #{tg}", callback_data=f"rmtag:{file_db_id}:{folder_id}:{tg}",
                                              icon_custom_emoji_id=pei("cross"))])
    rows.append([InlineKeyboardButton(text="Удалить", callback_data=f"delfile:{file_db_id}:{folder_id}", icon_custom_emoji_id=pei("trash"))])
    rows.append([InlineKeyboardButton(text="Назад", callback_data=f"folder:{folder_id}:0", icon_custom_emoji_id=pei("back"))])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_group_view(group_id: int, files: list, page: int, total: int, info: dict) -> InlineKeyboardMarkup:
    rows = []
    total_pages = pages_total(total, GROUP_FILES_PER_PAGE)
    
    # Файлы
    for f in files:
        fid, file_id, ftype, fname, fsize, is_starred, caption = f
        short = fname[:22] + "…" if len(fname) > 22 else fname
        
        # Кнопка файла
        btn_file = InlineKeyboardButton(
            text=short,
            callback_data=f"file:{fid}:{info['folder_id']}",
            icon_custom_emoji_id=pei(file_pe_key(ftype))
        )
        # Кнопка удаления из пака
        btn_remove = InlineKeyboardButton(
            text="✕",
            callback_data=f"group_remove_file:{fid}:{group_id}:{page}",
            icon_custom_emoji_id=pei("cross")
        )
        # Кнопка полного удаления
        btn_delete = InlineKeyboardButton(
            text="🗑",
            callback_data=f"group_delete_file:{fid}:{group_id}:{page}",
            icon_custom_emoji_id=pei("trash")
        )
        
        rows.append([btn_file, btn_remove, btn_delete])

    # Пагинация
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton(
                text="◁",
                callback_data=f"group_view:{group_id}:{page-1}",
                icon_custom_emoji_id=pei("back")
            ))
        nav_row.append(InlineKeyboardButton(
            text=f"{page+1}/{total_pages}",
            callback_data="noop"
        ))
        if page + 1 < total_pages:
            nav_row.append(InlineKeyboardButton(
                text="▷",
                callback_data=f"group_view:{group_id}:{page+1}",
                icon_custom_emoji_id=pei("next")
            ))
        rows.append(nav_row)

    # Кнопки действий
    rows.append([
        InlineKeyboardButton(
            text="Скачать всё",
            callback_data=f"group_download:{group_id}",
            icon_custom_emoji_id=pei("download")
        )
    ])
    rows.append([
        InlineKeyboardButton(
            text="Добавить файлы",
            callback_data=f"upload_to_pack:{group_id}",
            icon_custom_emoji_id=pei("upload")
        ),
        InlineKeyboardButton(
            text="Переименовать",
            callback_data=f"rename_group:{group_id}",
            icon_custom_emoji_id=pei("pencil")
        )
    ])
    rows.append([
        InlineKeyboardButton(
            text="Удалить пак (файлы останутся)",
            callback_data=f"delete_group:{group_id}",
            icon_custom_emoji_id=pei("trash")
        )
    ])
    rows.append([
        InlineKeyboardButton(
            text="Удалить пак и все файлы",
            callback_data=f"delete_group_with_files:{group_id}",
            icon_custom_emoji_id=pei("trash")
        )
    ])
    rows.append([
        InlineKeyboardButton(
            text="Назад",
            callback_data=f"folder:{info['folder_id']}:0",
            icon_custom_emoji_id=pei("back")
        )
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# Other keyboards
def kb_move_copy_target(file_db_id, current_folder_id, folders, action="move"):
    rows = []; cb = "move_do" if action=="move" else "copy_do"
    for fid, name, is_public, _, _, _ in folders:
        if fid == current_folder_id: continue
        rows.append([InlineKeyboardButton(text=name, callback_data=f"{cb}:{file_db_id}:{fid}:{current_folder_id}", icon_custom_emoji_id=pei("folder"))])
    rows.append([InlineKeyboardButton(text="Отмена", callback_data=f"file:{file_db_id}:{current_folder_id}", icon_custom_emoji_id=pei("cross"))])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_search_results(results, query, page, total):
    rows = []
    for fid, _, ftype, fname, fsize, folder_name, folder_id, group_id in results:
        if group_id: continue
        short = fname[:20]+"…" if len(fname)>20 else fname
        rows.append([InlineKeyboardButton(text=f"{short} [{folder_name[:12]}]", callback_data=f"file:{fid}:{folder_id}", icon_custom_emoji_id=pei(file_pe_key(ftype)))])
    nav = []
    total_pages = pages_total(total, FILES_PER_PAGE)
    encoded = query.replace(":", "_")
    if page>0: nav.append(InlineKeyboardButton(text="◁", callback_data=f"search_page:{encoded}:{page-1}", icon_custom_emoji_id=pei("back")))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page+1<total_pages: nav.append(InlineKeyboardButton(text="▷", callback_data=f"search_page:{encoded}:{page+1}", icon_custom_emoji_id=pei("next")))
    if len(nav)>1: rows.append(nav)
    rows.append([InlineKeyboardButton(text="Новый поиск", callback_data="search_start", icon_custom_emoji_id=pei("search")),
                 InlineKeyboardButton(text="Назад", callback_data="main_menu", icon_custom_emoji_id=pei("back"))])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_confirm_delfolder(folder_id):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Да, удалить", callback_data=f"confirmdelfolder:{folder_id}", icon_custom_emoji_id=pei("check")),
                                                InlineKeyboardButton(text="Отмена", callback_data=f"folder:{folder_id}:0", icon_custom_emoji_id=pei("cross"))]])

def kb_cancel(back_cb: str = "main_menu"):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data=back_cb, icon_custom_emoji_id=pei("cross"))]])

def kb_back(back_cb: str):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data=back_cb, icon_custom_emoji_id=pei("back"))]])

def kb_naming_file(folder_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Оставить оригинальное имя", callback_data=f"keep_name:{folder_id}", icon_custom_emoji_id=pei("check"))],
        [InlineKeyboardButton(text="Отмена", callback_data=f"folder:{folder_id}:0", icon_custom_emoji_id=pei("cross"))]])

def kb_upload_continue(folder_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Готово — вернуться в папку", callback_data=f"folder:{folder_id}:0", icon_custom_emoji_id=pei("check"))]])

def kb_upload_continue_pack(group_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Готово — вернуться в пак", callback_data=f"return_to_pack:{group_id}",
                             icon_custom_emoji_id=pei("check"))
    ]])

def kb_choose_folder_for_upload(folders, page, total, action="file"):
    rows = []
    cb_prefix = "quick_pack_folder" if action == "pack" else "quick_upload_folder"
    for fid, name, is_public, _, _, _ in folders:
        icon = pei("unlock") if is_public else pei("folder")
        rows.append([InlineKeyboardButton(text=f"{name}", callback_data=f"{cb_prefix}:{fid}", icon_custom_emoji_id=icon)])
    nav = []
    total_pages = pages_total(total, FOLDERS_PER_PAGE)
    if page>0: nav.append(InlineKeyboardButton(text="◁", callback_data=f"quick_upload_page:{page-1}", icon_custom_emoji_id=pei("back")))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page+1<total_pages: nav.append(InlineKeyboardButton(text="▷", callback_data=f"quick_upload_page:{page+1}", icon_custom_emoji_id=pei("next")))
    if len(nav)>1: rows.append(nav)
    rows.append([InlineKeyboardButton(text="Создать папку", callback_data="create_folder_quick", icon_custom_emoji_id=pei("plus"))])
    rows.append([InlineKeyboardButton(text="Отмена", callback_data="main_menu", icon_custom_emoji_id=pei("cross"))])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# Admin keyboards
def kb_admin_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Статистика", callback_data="admin_stats", icon_custom_emoji_id=pei("stats")),
         InlineKeyboardButton(text="Пользователи", callback_data="admin_users:0", icon_custom_emoji_id=pei("people"))],
        [InlineKeyboardButton(text="Поиск юзера", callback_data="admin_search", icon_custom_emoji_id=pei("search")),
         InlineKeyboardButton(text="Выдать Premium", callback_data="admin_grant", icon_custom_emoji_id=pei("crown"))],
        [InlineKeyboardButton(text="Рассылка", callback_data="admin_broadcast", icon_custom_emoji_id=pei("broadcast")),
         InlineKeyboardButton(text="Топ юзеров", callback_data="admin_top", icon_custom_emoji_id=pei("trophy"))],
        [InlineKeyboardButton(text="Закрыть", callback_data="main_menu", icon_custom_emoji_id=pei("cross"))],
    ])

def kb_admin_users(users, page, total):
    rows = []
    for uid, username, first_name, is_premium, bonus, created_at in users:
        crown = "👑 " if is_premium else ""
        name = first_name or "Без имени"
        uname = f" @{username}" if username else ""
        rows.append([InlineKeyboardButton(text=f"{crown}{name}{uname}  (ID: {uid})", callback_data=f"admin_user:{uid}", icon_custom_emoji_id=pei("crown") if is_premium else pei("profile"))])
    nav = []
    total_pages = (total+9)//10
    if page>0: nav.append(InlineKeyboardButton(text="◁", callback_data=f"admin_users:{page-1}", icon_custom_emoji_id=pei("back")))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page+1<total_pages: nav.append(InlineKeyboardButton(text="▷", callback_data=f"admin_users:{page+1}", icon_custom_emoji_id=pei("next")))
    if len(nav)>1: rows.append(nav)
    rows.append([InlineKeyboardButton(text="◁  Назад", callback_data="admin_panel", icon_custom_emoji_id=pei("back"))])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_admin_user_detail(user_id, is_premium):
    pt = "🔓  Отключить Premium" if is_premium else "👑  Включить Premium"
    # ИСПРАВЛЕНО: теперь иконка соответствует действию (замок для отключения, корона для включения)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=pt, callback_data=f"admin_toggle_premium:{user_id}", icon_custom_emoji_id=pei("unlock") if is_premium else pei("crown"))],
        [InlineKeyboardButton(text="📁  Папки", callback_data=f"admin_user_folders:{user_id}", icon_custom_emoji_id=pei("folder")),
         InlineKeyboardButton(text="📄  Файлы", callback_data=f"admin_user_files:{user_id}", icon_custom_emoji_id=pei("doc"))],
        [InlineKeyboardButton(text="◁  К списку", callback_data="admin_users:0", icon_custom_emoji_id=pei("back")),
         InlineKeyboardButton(text="⬅️  Панель", callback_data="admin_panel", icon_custom_emoji_id=pei("home"))],
    ])

def kb_admin_cancel(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌  Отмена", callback_data="admin_panel", icon_custom_emoji_id=pei("cross"))]])
def kb_admin_confirm_broadcast(): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅  Отправить всем", callback_data="admin_broadcast_confirm", icon_custom_emoji_id=pei("check")),
                                                                                   InlineKeyboardButton(text="❌  Отмена", callback_data="admin_panel", icon_custom_emoji_id=pei("cross"))]])

# ──────────────────────────────────────────── Media group buffer
_media_group_buffer: Dict[str, List[dict]] = defaultdict(list)
_media_group_tasks: Dict[str, asyncio.Task] = {}
_media_group_user: Dict[str, int] = {}
_media_group_lock = asyncio.Lock()
MEDIA_GROUP_WAIT = 1.5


def _extract_file_info(message: Message):
    if message.document:
        d = message.document
        return d.file_id, "document", d.file_name or "document", d.file_size or 0
    if message.photo:
        p = message.photo[-1]
        return p.file_id, "photo", f"photo_{p.file_unique_id[:8]}.jpg", p.file_size or 0
    if message.video:
        v = message.video
        return v.file_id, "video", v.file_name or "video.mp4", v.file_size or 0
    if message.audio:
        a = message.audio
        name = a.file_name or f"{a.performer or 'Unknown'} — {a.title or 'audio'}.mp3"
        return a.file_id, "audio", name, a.file_size or 0
    if message.voice:
        v = message.voice
        return v.file_id, "voice", f"voice_{v.file_unique_id[:8]}.ogg", v.file_size or 0
    if message.animation:
        a = message.animation
        return a.file_id, "animation", a.file_name or "animation.gif", a.file_size or 0
    if message.video_note:
        vn = message.video_note
        return vn.file_id, "video_note", f"videonote_{vn.file_unique_id[:8]}.mp4", vn.file_size or 0
    if message.sticker:
        s = message.sticker
        return s.file_id, "sticker", f"sticker_{s.file_unique_id[:8]}.webp", s.file_size or 0
    return None


async def _media_group_timer(mg_id: str, state: FSMContext):
    await asyncio.sleep(MEDIA_GROUP_WAIT)
    await _finalize_media_group(mg_id, state)


async def _finalize_media_group(media_group_id: str, state: FSMContext):
    async with _media_group_lock:
        items = _media_group_buffer.pop(media_group_id, None)
        _media_group_tasks.pop(media_group_id, None)
        user_id = _media_group_user.pop(media_group_id, None)
        if not items or not user_id:
            return

        count = len(items)
        data = await state.get_data()
        folder_id = data.get("folder_id")
        current_state = await state.get_state()

        # 📂 Сценарий 1: Загрузка внутри открытой папки (старый флоу)
        if current_state == States.uploading.state and folder_id:
            await state.update_data(pending_group_files=items, pending_group_folder_id=folder_id)
            await state.set_state(States.naming_group)
            try:
                await bot.send_message(
                    user_id,
                    f"{pe('upload','')} Получена группа из <b>{count}</b> файлов.\n"
                    f"{pe('write','')} Введи общее название (файлы будут названы <i>название_1</i>, <i>название_2</i>…)\n"
                    f"Или нажми кнопку, чтобы сохранить оригинальные имена.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="Оставить оригинальные имена", callback_data="keep_group_original", icon_custom_emoji_id=pei("check"))],
                        [InlineKeyboardButton(text="Отмена", callback_data="cancel_group_naming", icon_custom_emoji_id=pei("cross"))],
                    ])
                )
            except Exception as e:
                logger.error(f"Error in _finalize_media_group (uploading): {e}")
        else:
            # 📦 Сценарий 2: Пак кинули просто в чат (новый флоу)
            await state.set_state(States.choosing_folder_quick_pack)
            await state.update_data(pending_group_files=items)
            folders, total = await folders_get(user_id, 0)
            try:
                await bot.send_message(
                    user_id,
                    f"{pe('archive','📦')} <b>Получена группа из {count} файлов.</b>\n"
                    f"Выберите папку, в которую сохранить этот пак:",
                    reply_markup=kb_choose_folder_for_upload(folders, 0, total)
                )
            except Exception as e:
                logger.error(f"Error in _finalize_media_group (quick pack): {e}")
                
async def handle_media_group_message(message: Message, state: FSMContext):
    """Returns True if message was added to a media group buffer, else False."""
    mg_id = message.media_group_id
    if not mg_id:
        return False

    user_id = message.from_user.id
    info = _extract_file_info(message)
    if not info:
        return False

    entry = {
        "file_id": info[0],
        "file_type": info[1],
        "file_name": info[2],
        "file_size": info[3],
        "user_id": user_id,
    }

    async with _media_group_lock:
        _media_group_buffer[mg_id].append(entry)
        _media_group_user[mg_id] = user_id
        if mg_id not in _media_group_tasks:
            task = asyncio.create_task(_media_group_timer(mg_id, state))
            _media_group_tasks[mg_id] = task
    return True


# ──────────────────────────────────────────── Handlers
@dp.update.middleware()
async def main_middleware(handler, event, data):
    user = None
    if hasattr(event, "message") and event.message:
        user = event.message.from_user
    elif hasattr(event, "callback_query") and event.callback_query:
        user = event.callback_query.from_user
    if user:
        await user_upsert(user.id, user.username, user.first_name or "")
        if not _check_rate(user.id):
            if hasattr(event, "callback_query") and event.callback_query:
                await event.callback_query.answer(f"{pe('clock','⏳')} Слишком много запросов. Подожди немного.", show_alert=True)
            elif hasattr(event, "message") and event.message:
                await event.message.answer(f"{pe('clock','⏳')} Слишком много запросов. Подожди минуту.", reply_to_message_id=event.message.message_id)
            return
    return await handler(event, data)


@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    args = message.text.split(maxsplit=1)
    arg = args[1] if len(args) > 1 else ""
    if arg.startswith("share_"):
        token = arg[6:]
        folder = await folder_get_by_token(token)
        if folder:
            if _is_expired(folder[5]):
                await message.answer(f"{pe('cross','❌')} <b>Ссылка истекла.</b>\n\nСрок действия этой публичной ссылки закончился.",
                                     reply_markup=await reply_main_menu(message.from_user.id), reply_to_message_id=message.message_id)
                return
            if folder[6]:
                await state.set_state(States.entering_password)
                await state.update_data(pub_folder_id=folder[0])
                await message.answer(f"{pe('lock','🔒')} <b>Папка защищена паролем</b>\n\nВведи пароль для доступа:",
                                     reply_markup=reply_cancel("Отмена"), reply_to_message_id=message.message_id)
                return
            await _show_public_folder(message, folder, 0)
            return
        await message.answer(f"{pe('cross','❌')} <b>Папка не найдена.</b>\n\nВозможно ссылка устарела или папка была закрыта.",
                             reply_markup=await reply_main_menu(message.from_user.id), reply_to_message_id=message.message_id)
        return
    referred_by = None
    if arg.startswith("ref_"):
        try: referred_by = int(arg[4:])
        except: pass
    if referred_by:
        await user_upsert(message.from_user.id, message.from_user.username, message.from_user.first_name or "", referred_by)
    name = message.from_user.first_name
    await message.answer(
        f"{pe('sparkle','✨')} <b>Привет, {name}!</b>\n\n"
        f"{pe('folder','📁')} Я твой личный <b>файловый менеджер</b> в Telegram.\n\n"
        f"<b>Что умею:</b>\n"
        f"{pe('folder','📁')} Папки и подпапки\n"
        f"{pe('upload','⬆️')} Загрузка любых файлов\n"
        f"{pe('link','🔗')} Загрузка по ссылке (HTTP/HTTPS)\n"
        f"{pe('archive','🗂')} Группировка файлов в паки\n"
        f"{pe('tag','🏷')} Теги и поиск\n"
        f"{pe('star','⭐️')} Избранное\n"
        f"{pe('unlock','🔓')} Публичные ссылки с паролем\n"
        f"{pe('people','👥')} Реферальная программа\n\n"
        f"{pe('info','ℹ')} Подробнее — /help",
        reply_markup=await reply_main_menu(message.from_user.id),
        reply_to_message_id=message.message_id
    )

@dp.callback_query(F.data.startswith("share_link:"))
async def cb_share_link(callback: CallbackQuery):
    folder_id = int(callback.data.split(":")[1])
    folder = await folder_get(folder_id)
    if not folder or folder[1] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Папка не найдена", show_alert=True)
        return
    if not folder[3]:  # не публичная
        await callback.answer(f"{pe('lock','🔒')} Папка закрыта. Сначала откройте её кнопкой «Открыть».", show_alert=True)
        return
    # Проверяем, не истекла ли ссылка
    if _is_expired(folder[5]):
        await callback.answer(f"{pe('cross','❌')} Ссылка истекла. Откройте папку заново.", show_alert=True)
        return
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=share_{folder[4]}"
    await callback.message.answer(
        f"{pe('unlock','🔓')} <b>Публичная ссылка на папку</b>\n\n"
        f"{pe('folder','📁')} <b>{folder[2]}</b>\n\n"
        f"{pe('link','🔗')} <code>{link}</code>\n\n"
        f"{pe('clock','⌛️')} Ссылка действительна до: <code>{folder[5]}</code>\n"
        f"{pe('eye','👁')} Просмотров: <b>{folder[7]}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Закрыть", callback_data=f"folder:{folder_id}:0", icon_custom_emoji_id=pei("cross"))]
        ])
    )
    await callback.answer()

@dp.message(Command("premium"))
async def cmd_premium(message: Message):
    await message.answer(
        f"{pe('crown','👑')} <b>Premium</b>\n\n{pe('sparkle','✨')} Разблокирует:\n"
        f"{pe('folder','📁')} До <b>{MAX_FOLDERS_PREMIUM}</b> папок (вместо {MAX_FOLDERS_FREE})\n"
        f"{pe('doc','📄')} До <b>{MAX_FILES_PREMIUM}</b> файлов на папку\n"
        f"{pe('lightning','⚡')} Приоритетная поддержка\n\nДля активации обратитесь к @voblya_dev",
        reply_markup=await reply_main_menu(message.from_user.id), reply_to_message_id=message.message_id)

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        f"{pe('info','ℹ️')} <b>Помощь по использованию бота</b>\n\n"
        f"{pe('folder','📁')} <b>Папки и подпапки</b>\n"
        f"• Создавайте папки и вкладывайте их друг в друга\n"
        f"• Откройте папку → кнопка «Подпапка»\n\n"
        
        f"{pe('upload','⬆️')} <b>Загрузка файлов</b>\n"
        f"• Откройте папку → нажмите «Загрузить» → отправляйте файлы\n"
        f"• После каждого файла можно задать имя или оставить оригинальное\n"
        f"• Если отправить несколько фото/видео за раз — они соберутся в <b>пак</b>\n\n"
        
        f"{pe('link','🔗')} <b>Загрузка по ссылке</b>\n"
        f"• Нажмите «Загрузить по ссылке» → отправьте HTTP/HTTPS ссылку\n"
        f"• Бот скачает файл и предложит сохранить в папку\n"
        f"• <i>YouTube временно не поддерживается</i>\n\n"
        
        f"{pe('archive','🗂')} <b>Паки (группы файлов)</b>\n"
        f"• При загрузке нескольких файлов за раз создаётся пак\n"
        f"• В паке можно скачать всё сразу или удалить файлы по одному\n"
        f"• Добавляйте файлы в существующий пак через меню файла → «Добавить в пак»\n"
        f"• Перемещайте файлы между паками → «Переместить в другой пак»\n\n"
        
        f"{pe('tag','🏷')} <b>Теги</b>\n"
        f"• Откройте файл → «Добавить тег» → введите слово без #\n"
        f"• Поиск по тегам работает через кнопку «Теги» в главном меню\n\n"
        
        f"{pe('search','🔎')} <b>Поиск</b>\n"
        f"• Ищет по именам файлов и по тегам\n"
        f"• Доступен из главного меню или из любой папки\n\n"
        
        f"{pe('star','⭐️')} <b>Избранное</b>\n"
        f"• Откройте файл → «В избранное» → потом просматривайте через «Избранное» в меню\n\n"
        
        f"{pe('unlock','🔓')} <b>Публичные папки</b>\n"
        f"• Откройте папку → «Открыть» → получите ссылку на {SHARE_LINK_DAYS} дней\n"
        f"• Можно установить пароль → кнопка «Пароль»\n"
        f"• Просмотры папки видны владельцу\n\n"
        
        f"{pe('backup','⬇️')} <b>Бэкап и ZIP</b>\n"
        f"• В папке есть кнопки «Бэкап» (отправляет все файлы в чат) и «Скачать ZIP»\n\n"
        
        f"{pe('people','👥')} <b>Рефералы</b>\n"
        f"• Используйте /ref — получите личную ссылку\n"
        f"• За каждого друга, перешедшего по ссылке, +{REFERRAL_BONUS_FOLDERS} папок\n\n"
        
        f"{pe('crown','👑')} <b>Premium</b>\n"
        f"• /premium — подробнее о расширенных лимитах\n\n"
        
        f"{pe('robot','🤖')} <b>Остальное</b>\n"
        f"• Кнопка «Профиль» — ваша статистика и реферальная ссылка\n"
        f"• «Отмена» или /cancel — сбросить активное действие\n"
        f"• Если бот не отвечает — напишите @voblya_dev",
        reply_markup=await reply_main_menu(message.from_user.id),
        reply_to_message_id=message.message_id
    )

@dp.message(Command("ref"))
async def cmd_ref(message: Message):
    uid = message.from_user.id
    link = f"https://t.me/{(await bot.get_me()).username}?start=ref_{uid}"
    u = await user_get(uid)
    bonus = u[4] if u else 0
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (uid,)) as cur:
            ref_count = (await cur.fetchone())[0]
    await message.answer(
        f"{pe('people','👥')} <b>Реферальная программа</b>\n\n"
        f"{pe('gift','🎁')} За каждого приглашённого друга ты получаешь\n<b>+{REFERRAL_BONUS_FOLDERS} слотов</b> для папок!\n\n"
        f"{pe('link','🔗')} Твоя ссылка:\n<code>{link}</code>\n\n"
        f"{pe('people2','👥')} Приглашено: <b>{ref_count}</b>\n{pe('folder','📁')} Бонус папок: <b>+{bonus}</b>",
        reply_markup=await reply_main_menu(message.from_user.id), reply_to_message_id=message.message_id)


@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if not ADMIN_ID or message.from_user.id != ADMIN_ID: return
    await message.answer(f"{pe('crown','👑')} <b>Панель администратора</b>\n\n{pe('robot','🤖')} Добро пожаловать! Выберите действие:",
                         reply_markup=kb_admin_main(), reply_to_message_id=message.message_id)


# Public folder

async def _show_public_folder(message_or_cb, folder, page):
    folder_id = folder[0]
    name = folder[2]
    await folder_increment_views(folder_id)

    groups_list = await folder_groups_list(folder_id, folder[1])
    groups_dict = {g[0]: g[1] for g in groups_list}

    files, total = await files_get_public(folder_id, page)
    is_msg = isinstance(message_or_cb, Message)

    expires_str = folder[5] if len(folder) > 5 else None
    exp_note = ""
    if expires_str:
        try:
            exp = datetime.strptime(expires_str, "%Y-%m-%d %H:%M:%S")
            days_left = (exp - datetime.now(timezone.utc)).days
            exp_note = f"\n{pe('clock','⌛️')} Ссылка действует ещё <b>{days_left}</b> дн."
        except:
            pass

    text = (
        f"{pe('unlock','🔓')} <b>Публичная папка</b>\n"
        f"{pe('folder','📁')} <b>{name}</b>\n{pe('doc','📄')} Файлов: <b>{total}</b>{exp_note}\n\n"
    )
    if not files and not groups_list:
        text += f"{pe('info','ℹ')} Папка пуста."
    else:
        text += "Нажмите на файл, чтобы скачать:"

    rows = []
    grouped_files = defaultdict(list)

    for f in files:
        fid, file_id, ftype, fname, fsize, created_at, is_starred, caption, group_id = f[:9]
        if group_id and group_id in groups_dict:
            grouped_files[group_id].append(f)
        else:
            short = fname[:27] + "…" if len(fname) > 27 else fname
            rows.append([
                InlineKeyboardButton(
                    text=short,
                    callback_data=f"pub_dl:{fid}",
                    icon_custom_emoji_id=pei(file_pe_key(ftype))
                )
            ])

    # Кнопки для паков
    for gid, gfiles in grouped_files.items():
        group_name = groups_dict[gid]
        rows.append([
            InlineKeyboardButton(
                text=f" {group_name} ({len(gfiles)} файл.)",
                callback_data="noop",
                icon_custom_emoji_id=pei("archive")
            )
        ])
        for gf in gfiles:
            gf_id, gf_file_id, gtype, gname, *_ = gf[:4]
            s = gname[:20] + "…" if len(gname) > 20 else gname
            rows.append([
                InlineKeyboardButton(
                    text=f"    {s}",
                    callback_data=f"pub_dl:{gf_id}",
                    icon_custom_emoji_id=pei(file_pe_key(gtype))
                )
            ])

    # Пагинация
    nav = []
    total_pages = pages_total(total, FILES_PER_PAGE)
    if page > 0:
        nav.append(InlineKeyboardButton(text="◁", callback_data=f"pub_folder:{folder_id}:{page-1}", icon_custom_emoji_id=pei("back")))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page + 1 < total_pages:
        nav.append(InlineKeyboardButton(text="▷", callback_data=f"pub_folder:{folder_id}:{page+1}", icon_custom_emoji_id=pei("next")))
    if len(nav) > 1:
        rows.append(nav)

    # Кнопка "Скачать всё"
    if total > 0:
        rows.append([InlineKeyboardButton(
            text="Скачать все файлы",
            callback_data=f"pub_download_all:{folder_id}",
            icon_custom_emoji_id=pei("download")
        )])

    rows.append([InlineKeyboardButton(text="Главная", callback_data="main_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    if is_msg:
        await message_or_cb.answer(text, reply_markup=kb, reply_to_message_id=message_or_cb.message_id)
    else:
        await message_or_cb.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("pub_download_all:"))
async def cb_pub_download_all(callback: CallbackQuery):
    folder_id = int(callback.data.split(":")[1])
    folder = await folder_get(folder_id)
    if not folder:
        await callback.answer("Папка не найдена", show_alert=True)
        return

    # Собрать все файлы (игнорируем пагинацию)
    all_files = []
    page = 0
    while True:
        batch, total = await files_get_public(folder_id, page)
        all_files.extend(batch)
        page += 1
        if page * FILES_PER_PAGE >= total:
            break

    if not all_files:
        await callback.answer("Папка пуста", show_alert=True)
        return

    total_files = len(all_files)
    await callback.answer(f"⬇️ Начинаю отправку {total_files} файлов...")

    chat_id = callback.from_user.id
    status_msg = await bot.send_message(
        chat_id,
        f"{pe('download','⬇️')} Отправляю файлы из папки «{folder[2]}»..."
    )

    # Разделение на медиа и остальные
    media_list = []   # (file_id, file_type)
    other_list = []   # полные кортежи
    for f in all_files:
        ftype = f[2]
        if ftype in ("photo", "video"):
            media_list.append(f)
        else:
            other_list.append(f)

    sent = 0
    failed = 0

    # === Отправка медиа группами ===
    i = 0
    while i < len(media_list):
        group = media_list[i:i+10]   # до 10 в альбоме
        media = []
        for f in group:
            if f[2] == "photo":
                media.append(InputMediaPhoto(media=f[1]))
            else:
                media.append(InputMediaVideo(media=f[1]))
        try:
            await bot.send_media_group(chat_id, media)
            sent += len(group)
        except Exception as e:
            logger.error(f"Media group send error: {e}")
            failed += len(group)
        i += 10

        # Обновление прогресса
        progress = sent + failed
        bar = _progress_bar(progress, total_files)
        try:
            await status_msg.edit_text(
                f"{pe('download','⬇️')} Отправляю... <code>[{bar}]</code> {progress}/{total_files}"
            )
        except:
            pass
        await asyncio.sleep(0.5)   # пауза между альбомами

    # === Отправка остальных файлов по одному ===
    for f in other_list:
        try:
            await _send_file(status_msg, f[1], f[2], f[3])
            sent += 1
        except Exception as e:
            logger.warning(f"Send error in pub download all: {e}")
            failed += 1

        # Обновление прогресса каждые 5 файлов
        if (sent + failed) % 5 == 0:
            progress = sent + failed
            bar = _progress_bar(progress, total_files)
            try:
                await status_msg.edit_text(
                    f"{pe('download','⬇️')} Отправляю... <code>[{bar}]</code> {progress}/{total_files}"
                )
            except:
                pass
        await asyncio.sleep(0.3)

    # Итоговое сообщение
    await bot.send_message(
        chat_id,
        f"{pe('check','✅')} <b>Отправка завершена!</b>"
        + (f"\n❌ Ошибок: <b>{failed}</b>" if failed else "")
    )


GROUP_FILES_PER_PAGE = 20

@dp.callback_query(F.data.regexp(r"^pub_folder:\d+:\d+$"))
async def cb_pub_folder_page(callback: CallbackQuery, state: FSMContext):
    _, fid_s, page_s = callback.data.split(":")
    folder = await folder_get(int(fid_s))
    if not folder or not folder[3]: await callback.answer(f"{pe('cross','❌')} Папка недоступна", show_alert=True); return
    await _show_public_folder(callback, folder, int(page_s))
    await callback.answer()


@dp.callback_query(F.data.regexp(r"^pub_dl:\d+$"))
async def cb_pub_dl(callback: CallbackQuery):
    fid = int(callback.data.split(":")[1])
    f = await file_get(fid)
    if not f: await callback.answer(f"{pe('cross','❌')} Файл не найден", show_alert=True); return
    await callback.answer(f"{pe('download','⬇️')} Отправляю…")
    await _send_file(callback.message, f[1], f[2], f[3])

# ──────────────────────────────────────────── Group naming callbacks (удалены дубликаты)
@dp.callback_query(F.data == "keep_group_original", States.naming_group)
async def cb_keep_group_original(callback: CallbackQuery, state: FSMContext):
    await _save_group_files(callback.from_user.id, state, use_original_names=True)
    await callback.message.edit_text(f"{pe('check','✅')} Группа сохранена с оригинальными именами.")
    await _return_to_uploading(callback.message, state)
    await callback.answer()


@dp.callback_query(F.data == "cancel_group_naming", States.naming_group)
async def cb_cancel_group_naming(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(States.uploading)
    await callback.message.edit_text(f"{pe('cross','❌')} Сохранение группы отменено.")
    await callback.answer()


@dp.message(States.naming_group, F.text)
async def msg_group_name(message: Message, state: FSMContext):
    base_name = message.text.strip()
    if len(base_name) > 80:
        await message.answer(
            f"{pe('cross','❌')} Слишком длинное название (макс. 80 символов).",
            reply_to_message_id=message.message_id
        )
        return
    await _save_group_files(message.from_user.id, state, base_name=base_name)
    await message.answer(
        f"{pe('check','✅')} Группа сохранена как «<b>{base_name}</b>».",
        reply_to_message_id=message.message_id
    )
    await _return_to_uploading(message, state)

@dp.message(States.entering_password, F.text)
async def msg_enter_password(message: Message, state: FSMContext):
    data = await state.get_data()
    folder_id = data.get("pub_folder_id")
    folder = await folder_get(folder_id) if folder_id else None
    if not folder:
        await state.clear(); await message.answer(f"{pe('cross','❌')} Ошибка.", reply_markup=await reply_main_menu(message.from_user.id), reply_to_message_id=message.message_id)
        return
    entered = hashlib.sha256(message.text.strip().encode()).hexdigest()
    if entered == folder[6]:
        await state.clear(); await _show_public_folder(message, folder, 0)
    else:
        await message.answer(f"{pe('cross','❌')} <b>Неверный пароль.</b>\n\nПопробуй ещё раз:", reply_markup=reply_cancel("Отмена"), reply_to_message_id=message.message_id)


# Reply keyboard handlers
@dp.message(F.text == "Мои папки")
async def msg_my_folders(message: Message, state: FSMContext):
    await state.clear()
    folders, total = await folders_get(message.from_user.id)
    text = (f"{pe('folder','📁')} <b>Мои папки</b>\n\n{pe('stats','📊')} Всего: <b>{total}</b>  ·  Выбери папку:"
            if total else f"{pe('folder','📁')} <b>Мои папки</b>\n\n{pe('info','ℹ')} Папок пока нет. Создай первую!")
    await message.answer(text, reply_markup=kb_folders(folders, 0, total), reply_to_message_id=message.message_id)


@dp.message(F.text == "Создать папку")
async def msg_create_folder_reply(message: Message, state: FSMContext):
    count = await folders_count(message.from_user.id)
    limit = await user_max_folders(message.from_user.id)
    if count >= limit:
        await message.answer(f"{pe('cross','❌')} Достигнут лимит <b>{limit}</b> папок.\n{pe('crown','👑')} /premium для расширения",
                             reply_markup=await reply_main_menu(message.from_user.id), reply_to_message_id=message.message_id)
        return
    await state.set_state(States.creating_folder); await state.update_data(parent_id=None)
    await message.answer(f"{pe('pencil','✏️')} <b>Создание папки</b>\n\n{pe('write','📝')} Введи название новой папки:",
                         reply_markup=reply_cancel("Отмена"), reply_to_message_id=message.message_id)


@dp.message(F.text == "Профиль")
async def msg_profile_reply(message: Message):
    uid = message.from_user.id
    folders_c, files_c, total_size = await user_stats(uid)
    u = await user_get(uid)
    is_premium = bool(u[3]) if u else False
    bonus = u[4] if u else 0
    max_fold = await user_max_folders(uid)
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{uid}"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (uid,)) as cur:
            ref_count = (await cur.fetchone())[0]
    uu = message.from_user
    pct_folders = int(folders_c / max_fold * 10) if max_fold else 0
    bar_folders = "█" * pct_folders + "░" * (10 - pct_folders)
    await message.answer(
        f"{pe('profile','👤')} <b>Профиль</b>\n\n"
        f"<b>{uu.first_name}</b>\nID: <code>{uu.id}</code>\n"
        + (f"@{uu.username}\n" if uu.username else "")
        + f"Статус: {pe('crown','👑') if is_premium else pe('sparkle','✨')} <b>{'Premium' if is_premium else 'Free'}</b>\n\n"
        f"{pe('folder','📁')} Папки: <b>{folders_c}/{max_fold}</b>\n<code>[{bar_folders}]</code>"
        + (f" +{bonus} бонус\n" if bonus else "\n")
        + f"{pe('doc','📄')} Файлы: <b>{files_c}</b>\n{pe('box','📦')} Объём: <b>{format_size(total_size)}</b>\n\n"
        f"{pe('people','👥')} Рефералы: <b>{ref_count}</b>\n{pe('link','🔗')} <code>{ref_link}</code>",
        reply_markup=await reply_main_menu(message.from_user.id), reply_to_message_id=message.message_id)


@dp.message(F.text == "Отмена")
async def msg_cancel_state(message: Message, state: FSMContext):
    current = await state.get_state()
    if current:
        await state.clear()
        await message.answer(f"{pe('check','✅')} Действие отменено.", reply_markup=await reply_main_menu(message.from_user.id), reply_to_message_id=message.message_id)
    else:
        await message.answer("Нет активных действий.", reply_markup=await reply_main_menu(message.from_user.id), reply_to_message_id=message.message_id)


@dp.message(F.text == "Готово")
async def msg_done_upload(message: Message, state: FSMContext):
    user_id = message.from_user.id
    async with _media_group_lock:
        to_cancel = [mg_id for mg_id, task in _media_group_tasks.items() if _media_group_user.get(mg_id) == user_id]
        for mg_id in to_cancel:
            task = _media_group_tasks.pop(mg_id, None)
            if task: task.cancel()
            _media_group_buffer.pop(mg_id, None)
            _media_group_user.pop(mg_id, None)
    data = await state.get_data()
    folder_id = data.get("folder_id")
    upload_group_id = data.get("upload_group_id")
    await state.clear()
    if upload_group_id:
        info = await group_get(upload_group_id)
        if info and info["user_id"] == user_id:
            files, _ = await group_files_list(upload_group_id, page=None)
            text = (
                f"{pe('archive','🗂')} <b>Пак: {info['group_name']}</b>\n\n"
                f"{pe('doc','📄')} Файлов: <b>{info['files_count']}</b>  "
                f"{pe('box','📦')} <b>{format_size(info['total_size'])}</b>"
            )
            await message.answer(text, reply_markup=kb_group_view(upload_group_id, files, info),
                                 reply_to_message_id=message.message_id)
        else:
            await message.answer(f"{pe('cross','❌')} Пак не найден",
                                 reply_markup=await reply_main_menu(user_id),
                                 reply_to_message_id=message.message_id)
    elif folder_id:
        await _show_folder_by_message(message, folder_id)
    else:
        await message.answer(f"{pe('home','🏠')} Вернулись в главное меню.",
                             reply_markup=await reply_main_menu(user_id),
                             reply_to_message_id=message.message_id)


# Folder view (updated with groups)
def _group_files(files: list) -> tuple[list, dict]:
    singles = []
    groups = {}
    for f in files:
        group_id = f[8] if len(f) > 8 else None
        if group_id:
            if group_id not in groups:
                groups[group_id] = {'group_name': 'Группа', 'files': [], 'total_size': 0, 'count': 0}
            groups[group_id]['files'].append(f)
            groups[group_id]['total_size'] += f[4]
            groups[group_id]['count'] += 1
        else:
            singles.append(f)
    return singles, groups


async def get_group_names(group_ids: list[int]) -> dict[int, str]:
    if not group_ids: return {}
    placeholders = ",".join("?" for _ in group_ids)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"SELECT id, group_name FROM file_groups WHERE id IN ({placeholders})", group_ids) as cur:
            return {row[0]: row[1] for row in await cur.fetchall()}


async def _show_folder(callback: CallbackQuery, folder_id: int, page: int = 0, sort: str = "date", file_type: str | None = None, state: FSMContext = None):
    folder = await folder_get(folder_id)
    if not folder or folder[1] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Папка не найдена", show_alert=True)
        return
    is_public = bool(folder[3])
    files, total = await files_get(folder_id, callback.from_user.id, page, file_type=file_type, sort=sort)
    
    # Паки получаем напрямую из БД
    groups_info = await folder_groups_info(folder_id, callback.from_user.id)
    # На странице показываем только файлы БЕЗ пака
    singles = [f for f in files if len(f) <= 8 or f[8] is None]
    
    text = _folder_text(folder, total, sum(f[4] for f in files), is_public, bool(singles or groups_info))
    if state:
        await state.update_data(current_sort=sort, current_filter=file_type)
    await callback.message.edit_text(
        text, 
        reply_markup=kb_folder(folder_id, singles, page, total, is_public, sort=sort, file_type=file_type, subfolders=await subfolders_get(folder_id, callback.from_user.id), groups_info=groups_info)
    )

async def _show_folder_by_message(message: Message, folder_id: int, page: int = 0, sort: str = "date", file_type: str | None = None):
    folder = await folder_get(folder_id)
    if not folder or folder[1] != message.from_user.id:
        await message.answer(f"{pe('cross','❌')} Папка не найдена", reply_markup=await reply_main_menu(message.from_user.id), reply_to_message_id=message.message_id)
        return
    is_public = bool(folder[3])
    files, total = await files_get(folder_id, message.from_user.id, page, file_type=file_type, sort=sort)
    
    groups_info = await folder_groups_info(folder_id, message.from_user.id)
    singles = [f for f in files if len(f) <= 8 or f[8] is None]
    
    text = _folder_text(folder, total, sum(f[4] for f in files), is_public, bool(singles or groups_info))
    await message.answer(
        text, 
        reply_markup=kb_folder(folder_id, singles, page, total, is_public, sort=sort, file_type=file_type, subfolders=await subfolders_get(folder_id, message.from_user.id), groups_info=groups_info), 
        reply_to_message_id=message.message_id
    )

def _folder_text(folder, total, total_size, is_public, has_content) -> str:
    pub_badge = f"  {pe('globe','🌐')} <i>Публичная</i>" if is_public else ""
    views_line = f"{pe('eye','👁')} Просмотров: <b>{folder[7]}</b>\n" if is_public else ""
    if has_content:
        return (f"{pe('folder_open','📂')} <b>{folder[2]}</b>{pub_badge}\n\n"
                f"{pe('doc','📄')} Файлов: <b>{total}</b>  {pe('box','📦')} <b>{format_size(total_size)}</b>\n"
                f"{views_line}\nВыбери файл:")
    return (f"{pe('folder_open','📂')} <b>{folder[2]}</b>{pub_badge}\n\n{pe('info','ℹ')} Папка пуста. Нажми {pe('upload','⬆️')} <b>Загрузить</b>!")


# Callback main menu
@dp.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(f"{pe('home','🏠')} <b>Главное меню</b>\n\nИспользуй кнопки клавиатуры для навигации.", reply_markup=await reply_main_menu(callback.from_user.id))
    await callback.answer()


@dp.callback_query(F.data == "noop")
async def cb_noop(callback: CallbackQuery): await callback.answer()


@dp.callback_query(F.data == "help")
async def cb_help(callback: CallbackQuery):
    text = (
        f"{pe('info','ℹ️')} <b>Помощь по использованию бота</b>\n\n"
        f"{pe('folder','📁')} <b>Папки и подпапки</b>\n"
        f"• Создавайте папки и вкладывайте их друг в друга\n"
        f"• Откройте папку → кнопка «Подпапка»\n\n"
        f"{pe('upload','⬆️')} <b>Загрузка файлов</b>\n"
        f"• Откройте папку → нажмите «Загрузить» → отправляйте файлы\n"
        f"• После каждого файла можно задать имя или оставить оригинальное\n"
        f"• Если отправить несколько фото/видео за раз — они соберутся в <b>пак</b>\n\n"
        f"{pe('link','🔗')} <b>Загрузка по ссылке</b>\n"
        f"• Нажмите «Загрузить по ссылке» → отправьте HTTP/HTTPS ссылку\n"
        f"• Бот скачает файл и предложит сохранить в папку\n"
        f"• <i>YouTube временно не поддерживается</i>\n\n"
        f"{pe('archive','🗂')} <b>Паки (группы файлов)</b>\n"
        f"• При загрузке нескольких файлов за раз создаётся пак\n"
        f"• В паке можно скачать всё сразу или удалить файлы по одному\n"
        f"• Добавляйте файлы в существующий пак через меню файла → «Добавить в пак»\n"
        f"• Перемещайте файлы между паками → «Переместить в другой пак»\n\n"
        f"{pe('tag','🏷')} <b>Теги</b>\n"
        f"• Откройте файл → «Добавить тег» → введите слово без #\n"
        f"• Поиск по тегам работает через кнопку «Теги» в главном меню\n\n"
        f"{pe('search','🔎')} <b>Поиск</b>\n"
        f"• Ищет по именам файлов и по тегам\n"
        f"• Доступен из главного меню или из любой папки\n\n"
        f"{pe('star','⭐️')} <b>Избранное</b>\n"
        f"• Откройте файл → «В избранное» → потом просматривайте через «Избранное» в меню\n\n"
        f"{pe('unlock','🔓')} <b>Публичные папки</b>\n"
        f"• Откройте папку → «Открыть» → получите ссылку на {SHARE_LINK_DAYS} дней\n"
        f"• Можно установить пароль → кнопка «Пароль»\n"
        f"• Просмотры папки видны владельцу\n\n"
        f"{pe('backup','⬇️')} <b>Бэкап и ZIP</b>\n"
        f"• В папке есть кнопки «Бэкап» (отправляет все файлы в чат) и «Скачать ZIP»\n\n"
        f"{pe('people','👥')} <b>Рефералы</b>\n"
        f"• Используйте /ref — получите личную ссылку\n"
        f"• За каждого друга, перешедшего по ссылке, +{REFERRAL_BONUS_FOLDERS} папок\n\n"
        f"{pe('crown','👑')} <b>Premium</b>\n"
        f"• /premium — подробнее о расширенных лимитах\n\n"
        f"{pe('robot','🤖')} <b>Остальное</b>\n"
        f"• Кнопка «Профиль» — ваша статистика и реферальная ссылка\n"
        f"• «Отмена» или /cancel — сбросить активное действие\n"
        f"• Если бот не отвечает — напишите @voblya_dev"
    )
    await callback.message.edit_text(text, reply_markup=kb_back("main_menu"))
    await callback.answer()


# Folders list
@dp.callback_query(F.data.startswith("my_folders:"))
async def cb_my_folders(callback: CallbackQuery, state: FSMContext):
    await state.clear(); page = int(callback.data.split(":")[1])
    folders, total = await folders_get(callback.from_user.id, page)
    text = (f"{pe('folder','📁')} <b>Мои папки</b>\n\n{pe('stats','📊')} Всего: <b>{total}</b>  ·  Выбери папку:"
            if total else f"{pe('folder','📁')} <b>Мои папки</b>\n\n{pe('info','ℹ')} Папок пока нет. Создай первую!")
    await callback.message.edit_text(text, reply_markup=kb_folders(folders, page, total))
    await callback.answer()


# Create folder / subfolder
@dp.callback_query(F.data == "create_folder")
async def cb_create_folder(callback: CallbackQuery, state: FSMContext):
    count = await folders_count(callback.from_user.id); limit = await user_max_folders(callback.from_user.id)
    if count >= limit: await callback.answer(f"❌ Лимит {limit} папок. /premium для расширения", show_alert=True); return
    await state.set_state(States.creating_folder); await state.update_data(parent_id=None)
    await callback.message.edit_text(f"{pe('pencil','✏️')} <b>Создание папки</b>\n\n{pe('write','📝')} Введи название:", reply_markup=kb_cancel("my_folders:0"))
    await callback.answer()


@dp.callback_query(F.data.startswith("new_subfolder:"))
async def cb_new_subfolder(callback: CallbackQuery, state: FSMContext):
    parent_id = int(callback.data.split(":")[1])
    await state.set_state(States.creating_subfolder); await state.update_data(parent_id=parent_id)
    await callback.message.edit_text(f"{pe('pencil','✏️')} <b>Создание подпапки</b>\n\n{pe('write','📝')} Введи название:", reply_markup=kb_cancel(f"folder:{parent_id}:0"))
    await callback.answer()


async def _do_create_folder(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 50:
        await message.answer(f"{pe('cross','❌')} Слишком длинное название (макс. 50 символов).", reply_to_message_id=message.message_id)
        return
    
    data = await state.get_data()
    parent_id = data.get("parent_id")
    
    try:
        folder_id = await folder_create(message.from_user.id, name, parent_id)
    except ValueError as e:
        # Ловим дубликат и красиво выводим ошибку, не краша бота
        await message.answer(f"{pe('cross','❌')} {e}", reply_to_message_id=message.message_id)
        return
    except Exception as e:
        logger.error(f"Folder creation error: {e}")
        await message.answer(f"{pe('cross','❌')} Произошла ошибка при создании папки.", reply_to_message_id=message.message_id)
        return

    # ... далее ваш код без изменений (is_quick, move_file_db_id и т.д.) ...
    is_quick = data.get("is_quick_upload", False)
    folder_id = await folder_create(message.from_user.id, name, parent_id)

    # ----- Новый блок: перемещение файла после создания папки -----
    move_file_db_id = data.get("move_file_db_id")
    if move_file_db_id is not None:
        old_folder_id = data.get("move_old_folder_id")
        await file_move(move_file_db_id, message.from_user.id, folder_id)
        f = await file_get(move_file_db_id)
        new_folder = await folder_get(folder_id)
        files, total = await files_get(old_folder_id, message.from_user.id)
        old_folder = await folder_get(old_folder_id)
        await state.clear()
        await message.answer(
            f"{pe('check','✅')} <b>Файл перемещён!</b>\n\n"
            f"{file_emoji_pe(f[2])} <b>{f[3]}</b>\n{pe('next','➡️')} {pe('folder','📁')} <b>{new_folder[2]}</b>",
            reply_markup=kb_folder(old_folder_id, files, 0, total, bool(old_folder[3]) if old_folder else False),
            reply_to_message_id=message.message_id
        )
        return
    # ----- Конец нового блока -----

    if is_quick:
        pending = data.get("pending_file")
        await state.clear()
        await state.update_data(pending_file=pending)
        await state.set_state(States.choosing_folder_quick)
        folders, total = await folders_get(message.from_user.id, 0)
        file_id, file_type, file_name, file_size = pending
        await message.answer(
            f"{pe('check','✅')} Папка «<b>{name}</b>» создана!\n\n"
            f"{pe('folder','📁')} <b>Выберите папку для файла</b>\n\n"
            f"{file_emoji_pe(file_type)} <b>{file_name}</b>\n{pe('box','📦')} {format_size(file_size)}\n\nКуда сохранить?",
            reply_markup=kb_choose_folder_for_upload(folders, 0, total),
            reply_to_message_id=message.message_id
        )
    else:
        await state.clear()
        files, total = await files_get(folder_id, message.from_user.id)
        await message.answer(
            f"{pe('check','✅')} <b>{'Подп' if parent_id else 'П'}апка создана!</b>\n\n"
            f"{pe('folder','📁')} <b>{name}</b>\n\n{pe('upload','⬆️')} Нажми <b>Загрузить</b> и отправь что угодно!",
            reply_markup=kb_folder(folder_id, files, 0, total, False),
            reply_to_message_id=message.message_id
        )


@dp.message(States.creating_folder, F.text)
async def msg_folder_name(message: Message, state: FSMContext): await _do_create_folder(message, state)

@dp.message(States.creating_subfolder, F.text)
async def msg_subfolder_name(message: Message, state: FSMContext): await _do_create_folder(message, state)

# Folder view callback
@dp.callback_query(F.data.regexp(r"^folder:\d+:\d+$"))
async def cb_folder(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    _, folder_id_s, page_s = callback.data.split(":")
    try:
        await _show_folder(callback, int(folder_id_s), int(page_s), state=state)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await callback.answer()
        else:
            raise
    await callback.answer()

@dp.callback_query(F.data.startswith("flt:"))
async def cb_filter(callback: CallbackQuery):
    parts = callback.data.split(":"); folder_id = int(parts[1]); flt_key = parts[2]
    type_map = {"all": None, "doc": "document", "ph": "photo", "vid": "video", "aud": "audio"}
    try:
        await _show_folder(callback, folder_id, 0, file_type=type_map.get(flt_key))
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await callback.answer()  # просто игнорируем
        else:
            raise

@dp.callback_query(F.data.startswith("srt:"))
async def cb_sort(callback: CallbackQuery):
    parts = callback.data.split(":"); folder_id = int(parts[1]); sort_key = parts[2]
    try:
        await _show_folder(callback, folder_id, 0, sort=sort_key)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await callback.answer()
        else:
            raise

# Toggle public / password / views
@dp.callback_query(F.data.startswith("toggle_public:"))
async def cb_toggle_public(callback: CallbackQuery):
    folder_id = int(callback.data.split(":")[1])
    is_pub, token = await folder_toggle_public(folder_id, callback.from_user.id)
    folder = await folder_get(folder_id)
    if is_pub and folder:
        bot_info = await bot.get_me(); link = f"https://t.me/{bot_info.username}?start=share_{token}"
        await callback.message.edit_text(f"{pe('unlock','🔓')} <b>Папка открыта для всех!</b>\n\n{pe('link','🔗')} Публичная ссылка:\n<code>{link}</code>\n\n{pe('clock','⌛️')} Действует <b>{SHARE_LINK_DAYS}</b> дней\n{pe('key','🔑')} Можно добавить пароль кнопкой ниже.",
                                        reply_markup=kb_folder(folder_id, [], 0, 0, True))
    else:
        await callback.message.edit_text(f"{pe('lock','🔒')} <b>Папка закрыта.</b>\n\nПубличный доступ отключён.",
                                        reply_markup=kb_folder(folder_id, [], 0, 0, False))
    await callback.answer()

@dp.callback_query(F.data.startswith("set_password:"))
async def cb_set_password(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split(":")[1])
    await state.set_state(States.setting_password); await state.update_data(folder_id=folder_id)
    await callback.message.edit_text(f"{pe('lock','🔒')} <b>Пароль для папки</b>\n\nВведи пароль (или «<code>-</code>» чтобы убрать):", reply_markup=kb_cancel(f"folder:{folder_id}:0"))
    await callback.answer()

@dp.message(States.setting_password, F.text)
async def msg_set_password(message: Message, state: FSMContext):
    data = await state.get_data(); folder_id = data.get("folder_id"); pwd = message.text.strip()
    if pwd == "-": pwd = None
    await folder_set_password(folder_id, message.from_user.id, pwd)
    await state.clear()
    await message.answer(f"{pe('check','✅')} {'Пароль установлен!' if pwd else 'Пароль убран.'}", reply_markup=kb_back(f"folder:{folder_id}:0"), reply_to_message_id=message.message_id)

@dp.callback_query(F.data.startswith("folder_views:"))
async def cb_folder_views(callback: CallbackQuery):
    folder_id = int(callback.data.split(":")[1]); folder = await folder_get(folder_id)
    if not folder or folder[1] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True); return
    await callback.answer(f"👁 Просмотров: {folder[7]}", show_alert=True)

# Rename folder
@dp.callback_query(F.data.startswith("rename:"))
async def cb_rename(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split(":")[1]); folder = await folder_get(folder_id)
    await state.set_state(States.renaming_folder); await state.update_data(folder_id=folder_id)
    await callback.message.edit_text(f"{pe('pencil','✏️')} <b>Переименование папки</b>\n\nТекущее: <b>{folder[2] if folder else '?'}</b>\n\n{pe('write','📝')} Введи новое название:", reply_markup=kb_cancel(f"folder:{folder_id}:0"))
    await callback.answer()

@dp.message(States.renaming_folder, F.text)
async def msg_rename_folder(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 50: await message.answer(f"{pe('cross','❌')} Слишком длинное (макс. 50 символов).", reply_to_message_id=message.message_id); return
    data = await state.get_data(); folder_id = data["folder_id"]
    await folder_rename(folder_id, message.from_user.id, name); await state.clear()
    files, total = await files_get(folder_id, message.from_user.id)
    await message.answer(f"{pe('check','✅')} <b>Папка переименована!</b>\n\n{pe('folder','📁')} Новое название: <b>{name}</b>",
                         reply_markup=kb_folder(folder_id, files, 0, total, False), reply_to_message_id=message.message_id)

# Delete folder
@dp.callback_query(F.data.startswith("delfolder:"))
async def cb_delfolder_confirm(callback: CallbackQuery):
    folder_id = int(callback.data.split(":")[1]); folder = await folder_get(folder_id)
    if not folder or folder[1] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Папка не найдена", show_alert=True); return
    _, total = await files_get(folder_id, callback.from_user.id); sub_count = await subfolder_count(folder_id, callback.from_user.id)
    await callback.message.edit_text(f"{pe('trash','🗑')} <b>Удалить папку?</b>\n\n{pe('folder','📁')} <b>{folder[2]}</b>\n\n{pe('doc','📄')} Файлов: <b>{total}</b>\n{pe('folder_open','📂')} Подпапок: <b>{sub_count}</b>\n\n{pe('warning','❗️')} Все файлы и подпапки будут удалены <b>безвозвратно!</b>",
                                    reply_markup=kb_confirm_delfolder(folder_id))
    await callback.answer()

@dp.callback_query(F.data.startswith("confirmdelfolder:"))
async def cb_delfolder_confirmed(callback: CallbackQuery):
    folder_id = int(callback.data.split(":")[1])
    await folder_delete(folder_id, callback.from_user.id)
    folders, total = await folders_get(callback.from_user.id, 0)
    await callback.message.edit_text(f"{pe('check','✅')} <b>Папка удалена!</b>\n\n{pe('folder','📁')} Осталось папок: <b>{total}</b>", reply_markup=kb_folders(folders, 0, total))
    await callback.answer("✅ Удалено")

# Upload (with media group handling)
@dp.callback_query(F.data.startswith("upload:"))
async def cb_upload(callback: CallbackQuery, state: FSMContext):
    """Начало загрузки файлов"""
    folder_id_str = callback.data.split(":")[1] if ":" in callback.data else None
    
    if folder_id_str and folder_id_str.isdigit():
        # Загрузка в конкретную папку
        folder_id = int(folder_id_str)
        folder = await folder_get(folder_id)
        
        if not folder or folder[1] != callback.from_user.id:
            await callback.answer("❌ Папка не найдена или нет доступа", show_alert=True)
            return
        
        await state.set_state(States.uploading)
        await state.update_data(folder_id=folder_id)
        
        await callback.message.edit_text(
            f"{pe('upload','⬆️')} <b>Загрузка в папку: {folder[2]}</b>\n\n"
            f"{pe('info','ℹ')} Отправляйте файлы один за другим:\n"
            f"📄 Документы\n"
            f"🖼 Фото\n"
            f"🎥 Видео\n"
            f"🎶 Аудио\n"
            f"🎤 Голосовые сообщения\n"
            f"🎞 GIF/Анимации\n\n"
            f"{pe('check','✅')} Нажмите <b>Готово</b> когда закончите",
            reply_markup=kb_upload_continue(folder_id)
        )
        await callback.answer()
    else:
        # Выбор папки
        folders, _ = await folders_get(callback.from_user.id)
        
        if not folders:
            await callback.answer("❌ У вас нет папок. Создайте папку сначала!", show_alert=True)
            return
        
        rows = []
        for fid, fname, *_ in folders:
            rows.append([
                InlineKeyboardButton(
                    text=fname,
                    callback_data=f"upload:{fid}",
                    icon_custom_emoji_id=pei("folder")
                )
            ])
        
        rows.append([InlineKeyboardButton(
            text="Отмена",
            callback_data="main_menu",
            icon_custom_emoji_id=pei("cross")
        )])
        
        await callback.message.edit_text(
            f"{pe('upload','⬆️')} <b>Выберите папку для загрузки файлов:</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
        )
        await callback.answer()

@dp.callback_query(F.data.startswith("keep_name:"))
async def cb_keep_name(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data(); folder_id = data.get("folder_id"); pending = data.get("pending_file")
    if not pending or not folder_id: await callback.answer(f"{pe('cross','❌')} Ошибка", show_alert=True); return
    file_id, file_type, orig_name, file_size = pending
    await _save_file_final(callback.message, data, file_id, file_type, orig_name, file_size)
    await state.update_data(pending_file=None); await state.set_state(States.uploading)
    await callback.answer("✅ Сохранено")

async def _ask_name(message: Message, state: FSMContext, file_id: str, file_type: str, orig_name: str, file_size: int):
    await state.set_state(States.naming_file); await state.update_data(pending_file=(file_id, file_type, orig_name, file_size))
    folder_id = (await state.get_data()).get("folder_id")
    await message.answer(f"{pe('pencil','✏️')} <b>Как назвать файл?</b>\n\n{file_emoji_pe(file_type)} Оригинальное имя:\n<code>{orig_name}</code>\n\n{pe('write','📝')} Введи новое название или нажми кнопку:",
                         reply_markup=kb_naming_file(folder_id), reply_to_message_id=message.message_id)

async def _save_file_final(message: Message, data: dict, file_id: str, file_type: str, file_name: str, file_size: int):
    folder_id = data.get("folder_id")
    upload_group_id = data.get("upload_group_id")   # новое
    if not folder_id: return
    folder = await folder_get(folder_id)
    if not folder: return
    real_user_id = folder[1]
    _, total = await files_get(folder_id, real_user_id)
    limit = await user_max_files(real_user_id)
    if total >= limit:
        await message.answer(f"{pe('cross','❌')} Лимит <b>{limit}</b> файлов.\n{pe('crown','👑')} /premium для расширения", reply_to_message_id=message.message_id); return
    await file_save(real_user_id, folder_id, file_id, file_type, file_name, file_size, group_id=upload_group_id)
    # Выбираем правильную клавиатуру
    kb = kb_upload_continue_pack(upload_group_id) if upload_group_id else kb_upload_continue(folder_id)
    await message.answer(
        f"{pe('check','✅')} <b>Файл сохранён!</b>\n\n{pe('folder','📁')} Папка: <b>{folder[2]}</b>\n{file_emoji_pe(file_type)} <b>{file_name}</b>\n{pe('box','📦')} {format_size(file_size)}\n\n{pe('upload','⬆️')} Отправь ещё файл или нажми <b>Готово</b>.",
        reply_markup=kb,
        reply_to_message_id=message.message_id
    )

@dp.message(States.naming_file, F.text)
async def msg_name_file(message: Message, state: FSMContext):
    new_name = message.text.strip()
    if len(new_name) > 120: await message.answer(f"{pe('cross','❌')} Слишком длинное название (макс. 120 символов).", reply_to_message_id=message.message_id); return
    data = await state.get_data(); pending = data.get("pending_file")
    if not pending: await state.set_state(States.uploading); return
    file_id, file_type, orig_name, file_size = pending
    if "." not in new_name and "." in orig_name:
        ext = orig_name.rsplit(".", 1)[-1]; new_name = f"{new_name}.{ext}"
    await _save_file_final(message, data, file_id, file_type, new_name, file_size)
    await state.update_data(pending_file=None); await state.set_state(States.uploading)

async def _process_upload(message: Message, state: FSMContext, file_id: str, file_type: str, file_name: str, file_size: int):
    data = await state.get_data()
    if not data.get("folder_id"): await message.answer(f"{pe('info','ℹ')} Сначала открой папку и нажми {pe('upload','⬆️')} <b>Загрузить</b>.", reply_to_message_id=message.message_id); return
    await _ask_name(message, state, file_id, file_type, file_name, file_size)

# Upload handlers for each type, checking media_group_id first
@dp.message(States.uploading, F.photo)
async def up_photo(message: Message, state: FSMContext):
    if message.media_group_id and await handle_media_group_message(message, state): 
        return
    p = message.photo[-1]
    folder_id = (await state.get_data()).get("folder_id")
    if folder_id:
        await file_save(message.from_user.id, folder_id, p.file_id, "photo", 
                       f"photo_{p.file_unique_id[:8]}.jpg", p.file_size or 0)
        showToast('✅ Фото сохранено')

@dp.message(States.uploading, F.video)
async def up_video(message: Message, state: FSMContext):
    if message.media_group_id and await handle_media_group_message(message, state): 
        return
    v = message.video
    folder_id = (await state.get_data()).get("folder_id")
    if folder_id:
        await file_save(message.from_user.id, folder_id, v.file_id, "video",
                       v.file_name or "video.mp4", v.file_size or 0)

# ========== UPLOAD HANDLERS ==========
@dp.message(States.uploading, F.document)
async def up_document(message: Message, state: FSMContext):
    """Обработка документов при загрузке"""
    data = await state.get_data()
    folder_id = data.get("folder_id")
    user_id = message.from_user.id
    
    if not folder_id:
        await message.answer("❌ Ошибка: папка не выбрана", reply_markup=await reply_main_menu(user_id))
        await state.clear()
        return

    d = message.document
    file_name = d.file_name or "document"
    file_type = "document"
    file_size = d.file_size or 0
    file_id = d.file_id

    try:
        db_file_id = await file_save(user_id, folder_id, file_id, file_type, file_name, file_size)
        logger.info(f"✅ Document saved: {file_name} (ID: {db_file_id})")
        
        await message.answer(
            f"✅ <b>Документ сохранён!</b>\n\n"
            f"{pe('doc','📄')} <b>{file_name}</b>\n"
            f"{pe('box','📦')} {format_size(file_size)}",
            reply_markup=kb_upload_continue(folder_id)
        )
    except Exception as e:
        logger.error(f"Error saving document: {e}")
        await message.answer(f"❌ Ошибка: {e}", reply_markup=await reply_main_menu(user_id))
        await state.clear()


@dp.message(States.uploading, F.photo)
async def up_photo(message: Message, state: FSMContext):
    """Обработка фото при загрузке"""
    data = await state.get_data()
    folder_id = data.get("folder_id")
    user_id = message.from_user.id
    
    if not folder_id:
        await message.answer("❌ Ошибка: папка не выбрана", reply_markup=await reply_main_menu(user_id))
        await state.clear()
        return

    p = message.photo[-1]
    file_name = f"photo_{p.file_unique_id[:8]}.jpg"
    file_type = "photo"
    file_size = p.file_size or 0
    file_id = p.file_id

    try:
        db_file_id = await file_save(user_id, folder_id, file_id, file_type, file_name, file_size)
        logger.info(f"✅ Photo saved: {file_name} (ID: {db_file_id})")
        
        await message.answer(
            f"✅ <b>Фото сохранено!</b>\n\n"
            f"{pe('image','🖼')} <b>{file_name}</b>\n"
            f"{pe('box','📦')} {format_size(file_size)}",
            reply_markup=kb_upload_continue(folder_id)
        )
    except Exception as e:
        logger.error(f"Error saving photo: {e}")
        await message.answer(f"❌ Ошибка: {e}", reply_markup=await reply_main_menu(user_id))
        await state.clear()


@dp.message(States.uploading, F.video)
async def up_video(message: Message, state: FSMContext):
    """Обработка видео при загрузке"""
    data = await state.get_data()
    folder_id = data.get("folder_id")
    user_id = message.from_user.id
    
    if not folder_id:
        await message.answer("❌ Ошибка: папка не выбрана", reply_markup=await reply_main_menu(user_id))
        await state.clear()
        return

    v = message.video
    file_name = v.file_name or "video.mp4"
    file_type = "video"
    file_size = v.file_size or 0
    file_id = v.file_id

    try:
        db_file_id = await file_save(user_id, folder_id, file_id, file_type, file_name, file_size)
        logger.info(f"✅ Video saved: {file_name} (ID: {db_file_id})")
        
        await message.answer(
            f"✅ <b>Видео сохранено!</b>\n\n"
            f"{pe('video','🎥')} <b>{file_name}</b>\n"
            f"{pe('box','📦')} {format_size(file_size)}",
            reply_markup=kb_upload_continue(folder_id)
        )
    except Exception as e:
        logger.error(f"Error saving video: {e}")
        await message.answer(f"❌ Ошибка: {e}", reply_markup=await reply_main_menu(user_id))
        await state.clear()


@dp.message(States.uploading, F.audio)
async def up_audio(message: Message, state: FSMContext):
    """Обработка аудио при загрузке"""
    data = await state.get_data()
    folder_id = data.get("folder_id")
    user_id = message.from_user.id
    
    if not folder_id:
        await message.answer("❌ Ошибка: папка не выбрана", reply_markup=await reply_main_menu(user_id))
        await state.clear()
        return

    a = message.audio
    file_name = a.file_name or f"{a.performer or 'Unknown'} — {a.title or 'Audio'}.mp3"
    file_type = "audio"
    file_size = a.file_size or 0
    file_id = a.file_id

    try:
        db_file_id = await file_save(user_id, folder_id, file_id, file_type, file_name, file_size)
        logger.info(f"✅ Audio saved: {file_name} (ID: {db_file_id})")
        
        await message.answer(
            f"✅ <b>Аудио сохранено!</b>\n\n"
            f"{pe('audio','🎶')} <b>{file_name}</b>\n"
            f"{pe('box','📦')} {format_size(file_size)}",
            reply_markup=kb_upload_continue(folder_id)
        )
    except Exception as e:
        logger.error(f"Error saving audio: {e}")
        await message.answer(f"❌ Ошибка: {e}", reply_markup=await reply_main_menu(user_id))
        await state.clear()


@dp.message(States.uploading, F.voice)
async def up_voice(message: Message, state: FSMContext):
    """Обработка голосовых сообщений"""
    data = await state.get_data()
    folder_id = data.get("folder_id")
    user_id = message.from_user.id
    
    if not folder_id:
        await message.answer("❌ Ошибка: папка не выбрана", reply_markup=await reply_main_menu(user_id))
        await state.clear()
        return

    v = message.voice
    file_name = f"voice_{v.file_unique_id[:8]}.ogg"
    file_type = "voice"
    file_size = v.file_size or 0
    file_id = v.file_id

    try:
        db_file_id = await file_save(user_id, folder_id, file_id, file_type, file_name, file_size)
        logger.info(f"✅ Voice saved: {file_name} (ID: {db_file_id})")
        
        await message.answer(
            f"✅ <b>Голосовое сообщение сохранено!</b>\n\n"
            f"{pe('mic','🎤')} <b>{file_name}</b>\n"
            f"{pe('box','📦')} {format_size(file_size)}",
            reply_markup=kb_upload_continue(folder_id)
        )
    except Exception as e:
        logger.error(f"Error saving voice: {e}")
        await message.answer(f"❌ Ошибка: {e}", reply_markup=await reply_main_menu(user_id))
        await state.clear()


@dp.message(States.uploading, F.animation)
async def up_animation(message: Message, state: FSMContext):
    """Обработка GIF/анимаций"""
    data = await state.get_data()
    folder_id = data.get("folder_id")
    user_id = message.from_user.id
    
    if not folder_id:
        await message.answer("❌ Ошибка: папка не выбрана", reply_markup=await reply_main_menu(user_id))
        await state.clear()
        return

    a = message.animation
    file_name = a.file_name or "animation.gif"
    file_type = "animation"
    file_size = a.file_size or 0
    file_id = a.file_id

    try:
        db_file_id = await file_save(user_id, folder_id, file_id, file_type, file_name, file_size)
        logger.info(f"✅ Animation saved: {file_name} (ID: {db_file_id})")
        
        await message.answer(
            f"✅ <b>Анимация сохранена!</b>\n\n"
            f"{pe('film','🎞')} <b>{file_name}</b>\n"
            f"{pe('box','📦')} {format_size(file_size)}",
            reply_markup=kb_upload_continue(folder_id)
        )
    except Exception as e:
        logger.error(f"Error saving animation: {e}")
        await message.answer(f"❌ Ошибка: {e}", reply_markup=await reply_main_menu(user_id))
        await state.clear()


@dp.message(States.uploading)
async def up_unsupported(message: Message, state: FSMContext):
    """Обработка неподдерживаемых типов"""
    data = await state.get_data()
    folder_id = data.get("folder_id")
    user_id = message.from_user.id
    
    await message.answer(
        f"{pe('info','ℹ')} <b>Этот тип файла не поддерживается.</b>\n\n"
        f"Поддерживаемые типы:\n"
        f"📄 Документы\n"
        f"🖼 Фото\n"
        f"🎥 Видео\n"
        f"🎶 Аудио\n"
        f"🎤 Голосовые сообщения\n"
        f"🎞 GIF/Анимации\n\n"
        f"Отправьте нужный тип файла или нажмите <b>Готово</b>.",
        reply_markup=kb_upload_continue(folder_id) if folder_id else await reply_main_menu(user_id)
    )

# Group naming callbacks (оставлены только новые, дубликаты удалены)
@dp.callback_query(F.data == "keep_group_original", States.naming_group)
async def cb_keep_group_original(callback: CallbackQuery, state: FSMContext):
    await _save_group_files(callback.from_user.id, state, use_original_names=True)
    await callback.message.edit_text(f"{pe('check','✅')} Группа сохранена с оригинальными именами.")
    await _return_to_uploading(callback.message, state); await callback.answer()

@dp.callback_query(F.data == "cancel_group_naming", States.naming_group)
async def cb_cancel_group_naming(callback: CallbackQuery, state: FSMContext):
    await state.clear(); await state.set_state(States.uploading)
    await callback.message.edit_text(f"{pe('cross','❌')} Сохранение группы отменено."); await callback.answer()

@dp.message(States.naming_group, F.text)
async def msg_group_name(message: Message, state: FSMContext):
    base_name = message.text.strip()
    if len(base_name) > 80: await message.answer(f"{pe('cross','❌')} Слишком длинное название (макс. 80 символов).", reply_to_message_id=message.message_id); return
    await _save_group_files(message.from_user.id, state, base_name=base_name)
    await message.answer(f"{pe('check','✅')} Группа сохранена как «<b>{base_name}</b>».", reply_to_message_id=message.message_id)
    await _return_to_uploading(message, state)

async def _save_group_files(user_id: int, state: FSMContext, base_name: str = None, use_original_names: bool = False):
    data = await state.get_data(); items = data.get("pending_group_files", []); folder_id = data.get("pending_group_folder_id")
    if not items or not folder_id: return
    group_name = base_name or "Группа"
    # Check file limit
    _, current_files = await files_get(folder_id, user_id)
    limit = await user_max_files(user_id)
    if current_files + len(items) > limit:
        await bot.send_message(user_id, f"{pe('cross','❌')} Превышен лимит файлов в папке ({limit}). Группа не сохранена.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("INSERT INTO file_groups (user_id, folder_id, group_name) VALUES (?,?,?)", (user_id, folder_id, group_name))
        group_id = cur.lastrowid
        for idx, item in enumerate(items):
            file_id = item["file_id"]; file_type = item["file_type"]; orig_name = item["file_name"]; file_size = item["file_size"]
            if use_original_names: final_name = orig_name
            else:
                base_part = base_name or "file"; suffix = f"_{idx+1}" if len(items) > 1 else ""
                ext = os.path.splitext(orig_name)[1] if "." in orig_name else ""
                final_name = f"{base_part}{suffix}{ext}"
            await db.execute("INSERT INTO files (user_id, folder_id, file_id, file_type, file_name, file_size, group_id) VALUES (?,?,?,?,?,?,?)",
                             (user_id, folder_id, file_id, file_type, final_name, file_size, group_id))
        await db.commit()
    await state.update_data(pending_group_files=None)
    # ИСПРАВЛЕНО: очищаем также pending_group_folder_id, чтобы не оставалось в state
    await state.update_data(pending_group_folder_id=None)

async def _return_to_uploading(message: Message, state: FSMContext):
    await state.set_state(States.uploading)
    data = await state.get_data()
    folder_id = data.get("folder_id")
    upload_group_id = data.get("upload_group_id")
    if upload_group_id:
        kb = kb_upload_continue_pack(upload_group_id)
    else:
        kb = kb_upload_continue(folder_id) if folder_id else None
    await message.answer(
        f"{pe('upload','⬆️')} Можешь отправить ещё файлы или нажать <b>Готово</b>.",
        reply_markup=kb,
        reply_to_message_id=message.message_id
    )

# File view (updated with group_id)
@dp.callback_query(F.data.regexp(r"^file:\d+:\d+$"))
async def cb_file(callback: CallbackQuery):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s); folder_id = int(folder_id_s)
    f = await file_get(file_db_id)
    if not f: await callback.answer(f"{pe('cross','❌')} Файл не найден", show_alert=True); return
    tags = await file_get_tags(file_db_id)
    tag_line = ("  ".join(f"<code>#{t}</code>" for t in tags)) if tags else "нет"
    cap_line = f"\n\n{pe('write','📝')} <i>{f[8]}</i>" if f[8] else ""
    await callback.message.edit_text(
        f"{file_emoji_pe(f[2])} <b>{f[3]}</b>\n\n"
        f"{pe('doc','📄')} Тип: <b>{file_type_name(f[2])}</b>\n{pe('box','📦')} Размер: <b>{format_size(f[4])}</b>\n{pe('tag','🏷')} Теги: {tag_line}{cap_line}",
        reply_markup=kb_file(file_db_id, folder_id, bool(f[7]), tags, f[9]))
    await callback.answer()

# Download file
async def _send_file(message: Message, file_id: str, file_type: str, file_name: str):
    caption = f"{file_emoji_pe(file_type)} <b>{file_name}</b>"
    try:
        match file_type:
            case "photo": await message.answer_photo(file_id, caption=caption, reply_to_message_id=message.message_id)
            case "video": await message.answer_video(file_id, caption=caption, reply_to_message_id=message.message_id)
            case "audio": await message.answer_audio(file_id, caption=caption, reply_to_message_id=message.message_id)
            case "voice": await message.answer_voice(file_id, reply_to_message_id=message.message_id)
            case "animation": await message.answer_animation(file_id, caption=caption, reply_to_message_id=message.message_id)
            case "video_note": await message.answer_video_note(file_id, reply_to_message_id=message.message_id)
            case "sticker": await message.answer_sticker(file_id, reply_to_message_id=message.message_id)
            case _: await message.answer_document(file_id, caption=caption, reply_to_message_id=message.message_id)
    except Exception as e:
        logger.error(f"Send file error: {e}")
        await message.answer(f"{pe('cross','❌')} Ошибка отправки: <code>{e}</code>", reply_to_message_id=message.message_id)

@dp.callback_query(F.data.regexp(r"^download:\d+:\d+$"))
async def cb_download(callback: CallbackQuery):
    _, file_db_id_s, _ = callback.data.split(":"); f = await file_get(int(file_db_id_s))
    if not f: await callback.answer(f"{pe('cross','❌')} Файл не найден", show_alert=True); return
    await callback.answer(f"{pe('download','⬇️')} Отправляю…"); await _send_file(callback.message, f[1], f[2], f[3])

# Move / Copy (unchanged)
@dp.callback_query(F.data.regexp(r"^move_start:\d+:\d+$"))
async def cb_move_start(callback: CallbackQuery):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s); folder_id = int(folder_id_s)
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Файл не найден", show_alert=True); return
    folders, _ = await folders_get(callback.from_user.id)
    other = [fo for fo in folders if fo[0] != folder_id]
    if not other:
        # Нет других папок – предлагаем создать новую
        await callback.message.edit_text(
            f"{pe('folder','📁')} Нет других папок для перемещения.\nХотите создать новую папку?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="Создать папку",
                    callback_data=f"create_folder_move:{file_db_id}:{folder_id}",
                    icon_custom_emoji_id=pei("plus")
                )],
                [InlineKeyboardButton(
                    text="Отмена",
                    callback_data=f"file:{file_db_id}:{folder_id}",
                    icon_custom_emoji_id=pei("cross")
                )]
            ])
        )
        await callback.answer()
        return
    await callback.message.edit_text(
        f"{pe('move','↔️')} <b>Переместить файл</b>\n\n{file_emoji_pe(f[2])} <b>{f[3]}</b>\n\n{pe('folder','📁')} Выбери папку-назначение:",
        reply_markup=kb_move_copy_target(file_db_id, folder_id, folders, "move")
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("create_folder_move:"))
async def cb_create_folder_for_move(callback: CallbackQuery, state: FSMContext):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s)
    folder_id = int(folder_id_s)
    await state.set_state(States.creating_folder)
    await state.update_data(
        parent_id=None,
        is_quick_upload=False,
        move_file_db_id=file_db_id,
        move_old_folder_id=folder_id
    )
    await callback.message.edit_text(
        f"{pe('pencil','✏️')} <b>Создание папки</b>\n\nВведите название новой папки:",
        reply_markup=kb_cancel(f"file:{file_db_id}:{folder_id}")
    )
    await callback.answer()

@dp.callback_query(F.data.regexp(r"^move_do:\d+:\d+:\d+$"))
async def cb_move_do(callback: CallbackQuery):
    _, file_db_id_s, new_id_s, old_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s); new_id = int(new_id_s); old_id = int(old_id_s)
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True); return
    new_folder = await folder_get(new_id)
    if not new_folder or new_folder[1] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Папка не найдена", show_alert=True); return
    await file_move(file_db_id, callback.from_user.id, new_id)
    files, total = await files_get(old_id, callback.from_user.id)
    old_folder = await folder_get(old_id)
    await callback.message.edit_text(f"{pe('check','✅')} <b>Файл перемещён!</b>\n\n{file_emoji_pe(f[2])} <b>{f[3]}</b>\n{pe('next','➡️')} {pe('folder','📁')} <b>{new_folder[2]}</b>",
                                    reply_markup=kb_folder(old_id, files, 0, total, bool(old_folder[3]) if old_folder else False))
    await callback.answer("✅ Перемещено")

@dp.callback_query(F.data.regexp(r"^copy_start:\d+:\d+$"))
async def cb_copy_start(callback: CallbackQuery):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s); folder_id = int(folder_id_s)
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Файл не найден", show_alert=True); return
    folders, _ = await folders_get(callback.from_user.id)
    await callback.message.edit_text(
        f"{pe('copy','🔁')} <b>Копировать файл</b>\n\n{file_emoji_pe(f[2])} <b>{f[3]}</b>\n\n{pe('folder','📁')} Выбери папку-назначение:",
        reply_markup=kb_move_copy_target(file_db_id, folder_id, folders, "copy")
    )
    await callback.answer()

@dp.callback_query(F.data.regexp(r"^copy_do:\d+:\d+:\d+$"))
async def cb_copy_do(callback: CallbackQuery):
    _, file_db_id_s, new_id_s, old_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s); new_id = int(new_id_s); old_id = int(old_id_s)
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True); return
    new_folder = await folder_get(new_id)
    if not new_folder or new_folder[1] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Папка не найдена", show_alert=True); return
    await file_copy(file_db_id, callback.from_user.id, new_id)
    files, total = await files_get(old_id, callback.from_user.id)
    old_folder = await folder_get(old_id)
    await callback.message.edit_text(f"{pe('check','✅')} <b>Файл скопирован!</b>\n\n{file_emoji_pe(f[2])} <b>{f[3]}</b>\n{pe('next','➡️')} {pe('folder','📁')} <b>{new_folder[2]}</b>",
                                    reply_markup=kb_folder(old_id, files, 0, total, bool(old_folder[3]) if old_folder else False))
    await callback.answer("✅ Скопировано")

# Rename file, edit caption, star, tags, delete file – unchanged, but now include group_id in kb_file calls
@dp.callback_query(F.data.regexp(r"^renamefile:\d+:\d+$"))
async def cb_renamefile(callback: CallbackQuery, state: FSMContext):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s); folder_id = int(folder_id_s)
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True); return
    await state.set_state(States.renaming_file); await state.update_data(file_db_id=file_db_id, folder_id=folder_id)
    await callback.message.edit_text(f"{pe('pencil','✏️')} <b>Переименовать файл</b>\n\nТекущее имя: <code>{f[3]}</code>\n\n{pe('write','📝')} Введи новое название:", reply_markup=kb_cancel(f"file:{file_db_id}:{folder_id}"))
    await callback.answer()

@dp.message(States.renaming_file, F.text)
async def msg_rename_file(message: Message, state: FSMContext):
    new_name = message.text.strip()
    if len(new_name) > 120: await message.answer(f"{pe('cross','❌')} Слишком длинное (макс. 120 символов).", reply_to_message_id=message.message_id); return
    data = await state.get_data(); file_db_id = data["file_db_id"]; folder_id = data["folder_id"]
    f = await file_get(file_db_id)
    if f and "." in f[3] and "." not in new_name:
        ext = f[3].rsplit(".", 1)[-1]; new_name = f"{new_name}.{ext}"
    await file_rename(file_db_id, message.from_user.id, new_name); await state.clear()
    f2 = await file_get(file_db_id); tags = await file_get_tags(file_db_id)
    await message.answer(f"{pe('check','✅')} <b>Переименовано!</b>\n\n{file_emoji_pe(f2[2])} <b>{f2[3]}</b>",
                         reply_markup=kb_file(file_db_id, folder_id, bool(f2[7]) if f2 else False, tags, f2[9]), reply_to_message_id=message.message_id)

@dp.callback_query(F.data.regexp(r"^editcaption:\d+:\d+$"))
async def cb_editcaption(callback: CallbackQuery, state: FSMContext):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s); folder_id = int(folder_id_s)
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True); return
    await state.set_state(States.adding_caption); await state.update_data(file_db_id=file_db_id, folder_id=folder_id)
    current = f[8] or "—"
    await callback.message.edit_text(f"{pe('write','📝')} <b>Описание файла</b>\n\nТекущее: <i>{current}</i>\n\nВведи новое описание (или «<code>-</code>» чтобы убрать):",
                                    reply_markup=kb_cancel(f"file:{file_db_id}:{folder_id}"))
    await callback.answer()

@dp.message(States.adding_caption, F.text)
async def msg_add_caption(message: Message, state: FSMContext):
    caption = message.text.strip()
    if caption == "-": caption = ""
    data = await state.get_data(); file_db_id = data["file_db_id"]; folder_id = data["folder_id"]
    await file_set_caption(file_db_id, message.from_user.id, caption); await state.clear()
    f = await file_get(file_db_id); tags = await file_get_tags(file_db_id)
    await message.answer(f"{pe('check','✅')} Описание обновлено.", reply_markup=kb_file(file_db_id, folder_id, bool(f[7]) if f else False, tags, f[9]), reply_to_message_id=message.message_id)

@dp.callback_query(F.data.regexp(r"^star:\d+:\d+$"))
async def cb_star(callback: CallbackQuery):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s); folder_id = int(folder_id_s)
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True); return
    new_star = await file_toggle_star(file_db_id, callback.from_user.id)
    tags = await file_get_tags(file_db_id); f2 = await file_get(file_db_id)
    tag_line = ("  ".join(f"<code>#{t}</code>" for t in tags)) if tags else "нет"
    cap_line = f"\n\n{pe('write','📝')} <i>{f2[8]}</i>" if f2 and f2[8] else ""
    await callback.message.edit_text(
        f"{file_emoji_pe(f2[2])} <b>{f2[3]}</b>\n\n{pe('doc','📄')} Тип: <b>{file_type_name(f2[2])}</b>\n{pe('box','📦')} Размер: <b>{format_size(f2[4])}</b>\n{pe('tag','🏷')} Теги: {tag_line}{cap_line}",
        reply_markup=kb_file(file_db_id, folder_id, new_star, tags, f2[9]))
    await callback.answer("⭐️ Добавлено в избранное" if new_star else "✅ Убрано из избранного")

# Tags callbacks
@dp.callback_query(F.data.regexp(r"^addtag:\d+:\d+$"))
async def cb_addtag(callback: CallbackQuery, state: FSMContext):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s); folder_id = int(folder_id_s)
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True); return
    await state.set_state(States.adding_tag); await state.update_data(file_db_id=file_db_id, folder_id=folder_id)
    all_tags = await user_all_tags(callback.from_user.id)
    tags_hint = "Существующие теги: " + "  ".join(f"<code>#{t}</code>" for t in all_tags) if all_tags else ""
    await callback.message.edit_text(f"{pe('tag','🏷')} <b>Добавить тег</b>\n\nВведи тег: <code>работа</code> или <code>#договор</code>\n{tags_hint}",
                                    reply_markup=kb_cancel(f"file:{file_db_id}:{folder_id}"))
    await callback.answer()

@dp.message(States.adding_tag, F.text)
async def msg_add_tag(message: Message, state: FSMContext):
    tag_name = message.text.strip().lower().lstrip("#")
    if not tag_name or len(tag_name) > 32: await message.answer(f"{pe('cross','❌')} Некорректный тег (макс. 32 символа).", reply_to_message_id=message.message_id); return
    data = await state.get_data(); file_db_id = data["file_db_id"]; folder_id = data["folder_id"]
    await file_add_tag(file_db_id, tag_name); await state.clear()
    f = await file_get(file_db_id); tags = await file_get_tags(file_db_id)
    await message.answer(f"{pe('check','✅')} Тег <code>#{tag_name}</code> добавлен.", reply_markup=kb_file(file_db_id, folder_id, bool(f[7]) if f else False, tags, f[9]), reply_to_message_id=message.message_id)

@dp.callback_query(F.data.startswith("rmtag:"))
async def cb_rmtag(callback: CallbackQuery):
    parts = callback.data.split(":"); file_db_id = int(parts[1]); folder_id = int(parts[2]); tag_name = parts[3]
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True); return
    await file_remove_tag(file_db_id, tag_name)
    tags = await file_get_tags(file_db_id)
    tag_line = ("  ".join(f"<code>#{t}</code>" for t in tags)) if tags else "нет"
    cap_line = f"\n\n{pe('write','📝')} <i>{f[8]}</i>" if f[8] else ""
    await callback.message.edit_text(
        f"{file_emoji_pe(f[2])} <b>{f[3]}</b>\n\n{pe('doc','📄')} Тип: <b>{file_type_name(f[2])}</b>\n{pe('box','📦')} Размер: <b>{format_size(f[4])}</b>\n{pe('tag','🏷')} Теги: {tag_line}{cap_line}",
        reply_markup=kb_file(file_db_id, folder_id, bool(f[7]), tags, f[9]))
    await callback.answer(f"✅ Тег #{tag_name} удалён")

# Delete file
@dp.callback_query(F.data.regexp(r"^delfile:\d+:\d+$"))
async def cb_delfile(callback: CallbackQuery):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s); folder_id = int(folder_id_s)
    await file_delete(file_db_id, callback.from_user.id)
    files, total = await files_get(folder_id, callback.from_user.id)
    folder = await folder_get(folder_id); total_size = sum(f[4] for f in files)
    if not folder: await callback.answer(f"{pe('cross','❌')} Папка не найдена", show_alert=True); return
    await callback.message.edit_text(f"{pe('check','✅')} <b>Файл удалён</b>\n\n{pe('folder','📁')} <b>{folder[2]}</b>\n{pe('doc','📄')} Файлов: <b>{total}</b>  {pe('box','📦')} <b>{format_size(total_size) if files else '0 B'}</b>",
                                    reply_markup=kb_folder(folder_id, files, 0, total, bool(folder[3])))
    await callback.answer("✅ Удалено")

# Search, recent, starred
@dp.callback_query(F.data == "search_start")
async def cb_search_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(States.searching)
    await callback.message.edit_text(f"{pe('search','🔎')} <b>Поиск файлов</b>\n\n{pe('write','📝')} Введи название или часть названия:", reply_markup=kb_cancel("main_menu"))
    await callback.answer()

@dp.message(States.searching, F.text)
async def msg_search(message: Message, state: FSMContext):
    query = message.text.strip(); await state.clear()
    results, total = await files_search(message.from_user.id, query)
    if not results:
        await message.answer(f"{pe('cross','❌')} <b>Ничего не найдено</b>\n\nПо запросу «<b>{query}</b>» файлов нет.",
                             reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔎  Новый поиск", callback_data="search_start", icon_custom_emoji_id=pei("search")),
                                                                                    InlineKeyboardButton(text="◁  Назад", callback_data="main_menu", icon_custom_emoji_id=pei("back"))]]),
                             reply_to_message_id=message.message_id)
        return
    await message.answer(f"{pe('search','🔎')} <b>Результаты: «{query}»</b>\n\n{pe('stats','📊')} Найдено: <b>{total}</b>  ·  Выбери файл:",
                         reply_markup=kb_search_results(results, query, 0, total), reply_to_message_id=message.message_id)

@dp.callback_query(F.data.startswith("search_page:"))
async def cb_search_page(callback: CallbackQuery):
    parts = callback.data.split(":"); query = parts[1].replace("_", ":"); page = int(parts[2])
    results, total = await files_search(callback.from_user.id, query, page)
    await callback.message.edit_text(f"{pe('search','🔎')} <b>Результаты: «{query}»</b>\n\n{pe('stats','📊')} Найдено: <b>{total}</b>  ·  Выбери файл:",
                                    reply_markup=kb_search_results(results, query, page, total))
    await callback.answer()

@dp.callback_query(F.data == "recent_files")
async def cb_recent_files(callback: CallbackQuery):
    files = await files_recent(callback.from_user.id)
    if not files:
        await callback.message.edit_text(f"{pe('recent','🕓')} <b>Недавние файлы</b>\n\n{pe('info','ℹ')} Нет недавних файлов.", reply_markup=kb_back("main_menu")); await callback.answer(); return
    rows = []
    for fid, _, ftype, fname, fsize, folder_name, folder_id, is_starred, group_id in files:
        if group_id: continue
        star = "⭐️  " if is_starred else ""
        short = fname[:20] + "…" if len(fname) > 20 else fname
        rows.append([InlineKeyboardButton(text=f"{star}{file_emoji(ftype)}  {short}  [{folder_name[:10]}]", callback_data=f"file:{fid}:{folder_id}", icon_custom_emoji_id=pei(file_pe_key(ftype)))])
    rows.append([InlineKeyboardButton(text="◁  Назад", callback_data="main_menu", icon_custom_emoji_id=pei("back"))])
    await callback.message.edit_text(f"{pe('recent','🕓')} <b>Недавние файлы</b>\n\n{pe('stats','📊')} Последние <b>{len(files)}</b> файлов:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await callback.answer()

@dp.callback_query(F.data == "starred_files")
async def cb_starred_files(callback: CallbackQuery):
    files = await files_starred(callback.from_user.id)
    if not files:
        await callback.message.edit_text(f"{pe('star','⭐️')} <b>Избранное</b>\n\n{pe('info','ℹ')} Нет файлов в избранном.\nОткрой файл и нажми {pe('star','⭐️')} В избранное.", reply_markup=kb_back("main_menu")); await callback.answer(); return
    rows = []
    for fid, _, ftype, fname, fsize, folder_name, folder_id, group_id in files:
        if group_id: continue
        short = fname[:20] + "…" if len(fname) > 20 else fname
        rows.append([InlineKeyboardButton(text=f"⭐️  {file_emoji(ftype)}  {short}  [{folder_name[:10]}]", callback_data=f"file:{fid}:{folder_id}", icon_custom_emoji_id=pei(file_pe_key(ftype)))])
    rows.append([InlineKeyboardButton(text="◁  Назад", callback_data="main_menu", icon_custom_emoji_id=pei("back"))])
    await callback.message.edit_text(f"{pe('star','⭐️')} <b>Избранное</b>\n\n{pe('stats','📊')} Файлов: <b>{len(files)}</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await callback.answer()

# Profile callback
@dp.callback_query(F.data == "profile")
async def cb_profile(callback: CallbackQuery):
    uid = callback.from_user.id
    folders_c, files_c, total_size = await user_stats(uid)
    u = await user_get(uid)
    is_premium = bool(u[3]) if u else False; bonus = u[4] if u else 0; max_fold = await user_max_folders(uid)
    bot_info = await bot.get_me(); ref_link = f"https://t.me/{bot_info.username}?start=ref_{uid}"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (uid,)) as cur: ref_count = (await cur.fetchone())[0]
    uu = callback.from_user
    pct = int(folders_c / max_fold * 10) if max_fold else 0; bar = "█" * pct + "░" * (10 - pct)
    await callback.message.edit_text(
        f"{pe('profile','👤')} <b>Профиль</b>\n\n"
        f"<b>{uu.first_name}</b>\nID: <code>{uu.id}</code>\n"
        + (f"@{uu.username}\n" if uu.username else "")
        + f"Статус: {pe('crown','👑') if is_premium else pe('sparkle','✨')} <b>{'Premium' if is_premium else 'Free'}</b>\n\n"
        f"{pe('folder','📁')} Папки: <b>{folders_c}/{max_fold}</b>\n<code>[{bar}]</code>"
        + (f" +{bonus} бонус\n" if bonus else "\n")
        + f"{pe('doc','📄')} Файлы: <b>{files_c}</b>\n{pe('box','📦')} Объём: <b>{format_size(total_size)}</b>\n\n"
        f"{pe('people','👥')} Рефералы: <b>{ref_count}</b>\n{pe('link','🔗')} <code>{ref_link}</code>",
        reply_markup=kb_back("main_menu"))
    await callback.answer()

# Backup (unchanged)
@dp.callback_query(F.data.startswith("backup_folder:"))
async def cb_backup_folder(callback: CallbackQuery):
    folder_id = int(callback.data.split(":")[1]); folder = await folder_get(folder_id)
    if not folder or folder[1] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True); return
    await callback.answer("⏳ Начинаю бэкап…")
    all_files = []; page = 0
    while True:
        batch, _ = await files_get(folder_id, callback.from_user.id, page); 
        if not batch: break
        all_files.extend(batch); page += 1
        if page > 50: break
    if not all_files: await callback.message.answer(f"{pe('info','ℹ')} Папка пуста — нечего бэкапить."); return
    chat_id = callback.from_user.id
    status_msg = await bot.send_message(chat_id, f"{pe('backup','⬇️')} <b>Бэкап папки «{folder[2]}»</b>\n\n{pe('doc','📄')} Файлов: <b>{len(all_files)}</b>\n{_progress_bar(0, len(all_files))} 0/{len(all_files)}")
    sent = failed = 0
    for i, f in enumerate(all_files):
        try:
            await _send_file(status_msg, f[1], f[2], f[3]); sent += 1
        except Exception as e:
            logger.warning(f"Backup file error: {e}"); failed += 1
        if (i+1) % 5 == 0:
            bar = _progress_bar(i+1, len(all_files))
            try: await status_msg.edit_text(f"{pe('backup','⬇️')} <b>Бэкап папки «{folder[2]}»</b>\n\n<code>[{bar}]</code> {i+1}/{len(all_files)}\n✅ {sent}  ❌ {failed}")
            except: pass
        await asyncio.sleep(0.3)
    await bot.send_message(chat_id, f"{pe('check','✅')} <b>Бэкап завершён!</b>\n\n{pe('doc','📄')} Отправлено: <b>{sent}</b> / {len(all_files)}" + (f"\n{pe('cross','❌')} Ошибок: <b>{failed}</b>" if failed else ""))

# ZIP export
async def export_folder_zip(folder_id: int, user_id: int) -> tuple[io.BytesIO, int]:
    zip_buffer = io.BytesIO(); total_added = 0
    async def add_folder_to_zip(fid, rel_path, zipf):
        nonlocal total_added
        all_files = []; page = 0
        while True:
            batch, _ = await files_get(fid, user_id, page); 
            if not batch: break
            all_files.extend(batch); page += 1
            if page > 50: break
        seen_names = {}
        for f in all_files:
            file_db_id, file_id, file_name = f[0], f[1], f[3]
            if file_name in seen_names:
                base, ext = os.path.splitext(file_name); unique_name = f"{base}_{file_db_id}{ext}"
            else: unique_name = file_name
            seen_names[file_name] = seen_names.get(file_name, 0) + 1
            archive_path = f"{rel_path}/{unique_name}" if rel_path else unique_name
            try:
                tg_file = await bot.get_file(file_id); file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{tg_file.file_path}"
                async with aiohttp.ClientSession() as session:
                    async with session.get(file_url) as resp:
                        if resp.status == 200:
                            file_bytes = await resp.read()
                            zipf.writestr(archive_path, file_bytes)
                            total_added += 1
                        else: logger.warning(f"HTTP {resp.status} for {file_name}")
            except Exception as e: logger.warning(f"Download error {file_name}: {e}")
        for sf in await subfolders_get(fid, user_id):
            new_rel = f"{rel_path}/{sf[1]}" if rel_path else sf[1]
            await add_folder_to_zip(sf[0], new_rel, zipf)
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        await add_folder_to_zip(folder_id, "", zf)
    logger.info(f"ZIP ready: {total_added} files")
    zip_buffer.seek(0); return zip_buffer, total_added

@dp.callback_query(F.data.startswith("export_zip:"))
async def cb_export_zip(callback: CallbackQuery):
    folder_id = int(callback.data.split(":")[1]); folder = await folder_get(folder_id)
    if not folder or folder[1] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Папка не найдена", show_alert=True); return
    await callback.answer("⏳ Создаю архив…")
    zip_buffer, file_count = await export_folder_zip(folder_id, callback.from_user.id)
    if file_count == 0: await callback.message.answer(f"{pe('info','ℹ')} В папке нет файлов."); return
    zip_file = BufferedInputFile(zip_buffer.getvalue(), filename=f"{folder[2]}.zip")
    await callback.message.answer_document(zip_file, caption=f"{pe('archive','🗂')} Архив папки {folder[2]} ({file_count} файлов)")

# Quick upload, outside state
@dp.callback_query(F.data.startswith("quick_upload_page:"))
async def cb_quick_upload_page(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split(":")[1]); pending = (await state.get_data()).get("pending_file")
    if not pending: await callback.answer("Ошибка: файл не найден", show_alert=True); return
    folders, total = await folders_get(callback.from_user.id, page)
    await callback.message.edit_reply_markup(reply_markup=kb_choose_folder_for_upload(folders, page, total))
    await callback.answer()

@dp.callback_query(F.data.startswith("quick_upload_folder:"), States.choosing_folder_quick)
async def cb_quick_upload_folder(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split(":")[1]); data = await state.get_data(); pending = data.get("pending_file")
    if not pending: await callback.answer("Ошибка: файл не найден", show_alert=True); return
    folder = await folder_get(folder_id)
    if not folder or folder[1] != callback.from_user.id: await callback.answer(f"{pe('cross','❌')} Папка недоступна", show_alert=True); return
    await state.update_data(folder_id=folder_id); await state.set_state(States.naming_file)
    file_id, file_type, orig_name, file_size = pending
    await callback.message.edit_text(f"{pe('pencil','✏️')} <b>Как назвать файл?</b>\n\n{file_emoji_pe(file_type)} Оригинальное имя:\n<code>{orig_name}</code>\n\n{pe('write','📝')} Введи новое название или нажми кнопку:", reply_markup=kb_naming_file(folder_id))
    await callback.answer()

@dp.callback_query(F.data == "create_folder_quick")
async def cb_create_folder_quick(callback: CallbackQuery, state: FSMContext):
    count = await folders_count(callback.from_user.id); limit = await user_max_folders(callback.from_user.id)
    if count >= limit: await callback.answer(f"❌ Лимит {limit} папок. /premium для расширения", show_alert=True); return
    await state.set_state(States.creating_folder); await state.update_data(parent_id=None, is_quick_upload=True)
    await callback.message.edit_text(f"{pe('pencil','✏️')} <b>Создание папки</b>\n\n{pe('write','📝')} Введи название:", reply_markup=kb_cancel("main_menu"))
    await callback.answer()

@dp.message(~F.text)
async def any_file_outside_state(message: Message, state: FSMContext):
    current = await state.get_state()
    # Игнорируем, если уже в процессе загрузки или выбора
    if current in (States.uploading, States.naming_file, States.naming_group, 
                   States.choosing_folder_quick, States.choosing_folder_quick_pack):
        return

    # 🆕 Если это альбом/медиагруппа → отправляем в буфер и ждём остальные файлы
    if message.media_group_id:
        if await handle_media_group_message(message, state):
            return

    info = _extract_file_info(message)
    if not info:
        await message.answer(
            f"{pe('info','ℹ️')} <b>Как сохранить файл:</b>\n"
            f"1. {pe('folder','📁')} Открой нужную папку\n"
            f"2. {pe('upload','⬆️')} Нажми <b>Загрузить</b>\n"
            f"3. Отправь файл в чат\n"
            f"4. {pe('pencil','✏️')} Задай название или оставь оригинальное",
            reply_markup=await reply_main_menu(message.from_user.id), 
            reply_to_message_id=message.message_id
        )
        return

    file_id, file_type, file_name, file_size = info
    await state.set_state(States.choosing_folder_quick)
    await state.update_data(pending_file=(file_id, file_type, file_name, file_size))
    folders, total = await folders_get(message.from_user.id, 0)
    
    if total == 0:
        await message.answer(
            f"{pe('info','ℹ️')} Нет папок. Сначала создайте папку:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕  Создать папку", callback_data="create_folder", icon_custom_emoji_id=pei("plus"))],
                [InlineKeyboardButton(text="❌  Отмена", callback_data="main_menu", icon_custom_emoji_id=pei("cross"))]
            ]),
            reply_to_message_id=message.message_id
        )
        await state.clear()
        return

    await message.answer(
        f"{pe('folder','📁')} <b>Выберите папку для файла</b>\n"
        f"{file_emoji_pe(file_type)} <b>{file_name}</b>\n"
        f"{pe('box','📦')} {format_size(file_size)}\nКуда сохранить?",
        reply_markup=kb_choose_folder_for_upload(folders, 0, total),
        reply_to_message_id=message.message_id
    )

# Group download handler (kept for convenience)
@dp.callback_query(F.data.startswith("group_download:"))
async def cb_group_download(callback: CallbackQuery):
    group_id = int(callback.data.split(":")[1])
    
    # Получаем ВСЕ файлы пака (без пагинации)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, file_id, file_type, file_name FROM files WHERE group_id=? ORDER BY created_at ASC",
            (group_id,)
        ) as cur:
            all_files = await cur.fetchall()
    
    if not all_files:
        await callback.answer("Пак пуст.", show_alert=True)
        return

    await callback.answer(f"⬇️ Отправляю {len(all_files)} файлов...")
    chat_id = callback.from_user.id
    status_msg = await bot.send_message(chat_id, f"{pe('download','⬇️')} Отправка пака...")

    # Разделение на медиа и остальные
    media_list = []
    other_list = []
    for f in all_files:
        fid, file_id, ftype, fname = f
        if ftype in ("photo", "video"):
            media_list.append((fid, file_id, ftype))
        else:
            other_list.append((fid, file_id, ftype, fname))

    total_files = len(all_files)
    sent, failed = 0, 0

    # Отправка медиа группами по 10
    for i in range(0, len(media_list), 10):
        group = media_list[i:i+10]
        media = []
        for _, file_id, ftype in group:
            if ftype == "photo":
                media.append(InputMediaPhoto(media=file_id))
            else:
                media.append(InputMediaVideo(media=file_id))
        try:
            await bot.send_media_group(chat_id, media)
            sent += len(group)
        except Exception as e:
            logger.error(f"Media group error: {e}")
            failed += len(group)
        await asyncio.sleep(0.5)

    # Отправка остальных по одному
    for f in other_list:
        fid, file_id, ftype, fname = f
        try:
            await _send_file(status_msg, file_id, ftype, fname)
            sent += 1
        except Exception as e:
            logger.error(f"Send error: {e}")
            failed += 1
        await asyncio.sleep(0.3)

    await bot.send_message(
        chat_id,
        f"{pe('check','✅')} <b>Отправка завершена!</b>"
        + (f"\n❌ Ошибок: <b>{failed}</b>" if failed else "")
    )

# NEW: Group view & management callbacks (исправлены дубликаты, оставлены более полные версии)
@dp.callback_query(F.data.startswith("group_view:"))
async def cb_group_view(callback: CallbackQuery):
    # Формат callback: group_view:ID или group_view:ID:page
    parts = callback.data.split(":")
    group_id = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 0

    info = await group_get(group_id)
    if not info or info["user_id"] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Пак не найден", show_alert=True)
        return

    files, total = await group_files_list(group_id, page)
    text = (
        f"{pe('archive','🗂')} <b>Пак: {info['group_name']}</b>\n\n"
        f"{pe('doc','📄')} Файлов: <b>{info['files_count']}</b>  "
        f"{pe('box','📦')} <b>{format_size(info['total_size'])}</b>\n\n"
        "Выберите файл или действие:"
    )
    await callback.message.edit_text(text, reply_markup=kb_group_view(group_id, files, page, total, info))
    await callback.answer()


@dp.callback_query(F.data.startswith("group_remove_file:"))
async def cb_group_remove_file(callback: CallbackQuery):
    _, file_db_id_s, group_id_s, page_s = callback.data.split(":")
    file_db_id = int(file_db_id_s)
    group_id = int(group_id_s)
    page = int(page_s)

    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Файл не найден", show_alert=True)
        return
    await group_remove_file(file_db_id, callback.from_user.id)
    await callback.answer("✅ Убрано из пака")

    info = await group_get(group_id)
    if not info: return
    files, total = await group_files_list(group_id, page)
    await callback.message.edit_reply_markup(
        reply_markup=kb_group_view(group_id, files, page, total, info)
    )

@dp.callback_query(F.data.startswith("group_delete_file:"))
async def cb_group_delete_file(callback: CallbackQuery):
    _, file_db_id_s, group_id_s, page_s = callback.data.split(":")
    file_db_id = int(file_db_id_s)
    group_id = int(group_id_s)
    page = int(page_s)

    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Файл не найден", show_alert=True)
        return
    await group_delete_file(file_db_id, callback.from_user.id)
    await callback.answer("✅ Файл удалён")

    info = await group_get(group_id)
    if not info:
        await callback.message.edit_text(f"{pe('check','✅')} Пак пуст.")
        return
    files, total = await group_files_list(group_id, page)
    # Если страница оказалась пустой после удаления, перейти на предыдущую
    if not files and page > 0:
        page -= 1
        files, total = await group_files_list(group_id, page)
    await callback.message.edit_reply_markup(
        reply_markup=kb_group_view(group_id, files, page, total, info)
    )

@dp.callback_query(F.data.startswith("delete_group_with_files:"))
async def cb_confirm_delete_group_with_files(callback: CallbackQuery):
    group_id = int(callback.data.split(":")[1])
    info = await group_get(group_id)
    if not info or info["user_id"] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True)
        return
    await callback.message.edit_text(
        f"{pe('warning','❗️')} <b>Удалить пак и ВСЕ его файлы?</b>\n\n"
        f"Пак «{info['group_name']}» ({info['files_count']} файлов) будет удалён безвозвратно.\n"
        f"Файлы исчезнут из папки.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Да, удалить всё", callback_data=f"confirm_delete_group_with_files:{group_id}",
                                     icon_custom_emoji_id=pei("check")),
                InlineKeyboardButton(text="Отмена", callback_data=f"group_view:{group_id}",
                                     icon_custom_emoji_id=pei("cross"))
            ]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("add_to_group_confirm:"))
async def cb_confirm_add_to_group(callback: CallbackQuery):
    _, file_db_id_s, group_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s)
    group_id = int(group_id_s)
    
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Файл не найден", show_alert=True)
        return
    
    # Проверяем, что группа существует и принадлежит пользователю
    group = await group_get(group_id)
    if not group or group["user_id"] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Пак не найден или был удалён", show_alert=True)
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE files SET group_id=? WHERE id=? AND user_id=?",
                         (group_id, file_db_id, callback.from_user.id))
        await db.commit()
    
    tags = await file_get_tags(file_db_id)
    f2 = await file_get(file_db_id)
    await callback.message.edit_text(
        f"{file_emoji_pe(f2[2])} <b>{f2[3]}</b>\n\n"
        f"{pe('doc','📄')} Тип: <b>{file_type_name(f2[2])}</b>\n"
        f"{pe('box','📦')} Размер: <b>{format_size(f2[4])}</b>\n"
        f"{pe('tag','🏷')} Теги: {'  '.join(f'<code>#{t}</code>' for t in tags) if tags else 'нет'}"
        + (f"\n\n{pe('write','📝')} <i>{f2[8]}</i>" if f2[8] else ""),
        reply_markup=kb_file(file_db_id, f2[5], bool(f2[7]), tags, group_id)
    )
    await callback.answer("✅ Добавлено в пак")

@dp.callback_query(F.data.startswith("move_to_pack_confirm:"))
async def cb_confirm_move_to_pack(callback: CallbackQuery):
    _, file_db_id_s, new_group_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s)
    new_group_id = int(new_group_id_s)
    folder_id = int(folder_id_s)
    
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Файл не найден", show_alert=True)
        return
    
    # Проверяем, что целевая группа существует
    group = await group_get(new_group_id)
    if not group or group["user_id"] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Пак не найден или был удалён", show_alert=True)
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE files SET group_id=? WHERE id=? AND user_id=?",
                         (new_group_id, file_db_id, callback.from_user.id))
        await db.commit()
    
    tags = await file_get_tags(file_db_id)
    f2 = await file_get(file_db_id)
    await callback.message.edit_text(
        f"{file_emoji_pe(f2[2])} <b>{f2[3]}</b>\n\n"
        f"{pe('doc','📄')} Тип: <b>{file_type_name(f2[2])}</b>\n"
        f"{pe('box','📦')} Размер: <b>{format_size(f2[4])}</b>\n"
        f"{pe('tag','🏷')} Теги: {'  '.join(f'<code>#{t}</code>' for t in tags) if tags else 'нет'}"
        + (f"\n\n{pe('write','📝')} <i>{f2[8]}</i>" if f2[8] else ""),
        reply_markup=kb_file(file_db_id, folder_id, bool(f2[7]), tags, new_group_id)
    )
    await callback.answer("✅ Перемещено в другой пак")

@dp.callback_query(F.data.startswith("confirm_delete_group_with_files:"))
async def cb_delete_group_with_files(callback: CallbackQuery):
    group_id = int(callback.data.split(":")[1])
    info = await group_get(group_id)
    if not info or info["user_id"] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True)
        return
    folder_id = info["folder_id"]
    # Получаем список файлов пака
    files, _ = await group_files_list(group_id, page=None)
    for f in files:
        file_db_id = f[0]
        await file_delete(file_db_id, callback.from_user.id)  # удаляем каждый файл
    await group_delete(group_id, callback.from_user.id)  # удаляем саму группу
    await callback.message.edit_text(
        f"{pe('check','✅')} <b>Пак и все его файлы удалены.</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Вернуться в папку", callback_data=f"folder:{folder_id}:0",
                                  icon_custom_emoji_id=pei("back"))]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("rename_group:"))
async def cb_start_rename_group(callback: CallbackQuery, state: FSMContext):
    group_id = int(callback.data.split(":")[1])
    info = await group_get(group_id)
    if not info or info["user_id"] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True)
        return
    await state.set_state(States.renaming_group)
    await state.update_data(group_id=group_id, folder_id=info["folder_id"])
    await callback.message.edit_text(
        f"{pe('pencil','✏️')} <b>Переименовать пак</b>\n\nТекущее: <b>{info['group_name']}</b>\n\nВведите новое название:",
        reply_markup=kb_cancel(f"group_view:{group_id}")
    )
    await callback.answer()


@dp.message(States.renaming_group, F.text)
async def msg_rename_group(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 50:
        await message.answer(f"{pe('cross','❌')} Слишком длинное (макс. 50).", reply_to_message_id=message.message_id)
        return
    data = await state.get_data()
    group_id = data["group_id"]
    await group_rename(group_id, message.from_user.id, name)
    await state.clear()
    info = await group_get(group_id)
    files = await group_files_list(group_id)
    text = (
        f"{pe('archive','🗂')} <b>Пак: {info['group_name']}</b>\n\n"
        f"{pe('doc','📄')} Файлов: <b>{info['files_count']}</b>  "
        f"{pe('box','📦')} <b>{format_size(info['total_size'])}</b>"
    )
    await message.answer(
        f"{pe('check','✅')} <b>Пак переименован!</b>\n\n{text}",
        reply_markup=kb_group_view(group_id, files, info),
        reply_to_message_id=message.message_id
    )

@dp.message(States.creating_pack_quick, F.text)
async def msg_creating_pack_quick(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 50 or not name:
        await message.answer(f"{pe('cross','❌')} Некорректное название.", reply_to_message_id=message.message_id)
        return
    data = await state.get_data()
    # ИСПРАВЛЕНО: используем правильное имя ключа 'pack_file_db_id'
    file_db_id = data.get("pack_file_db_id")
    folder_id = data.get("pack_folder_id")
    action = data.get("pack_action")   # "add" или "move"
    if not file_db_id or not folder_id or not action:
        await message.answer(f"{pe('cross','❌')} Ошибка сессии.", reply_to_message_id=message.message_id)
        await state.clear()
        return
    await state.clear()

    # Создаём пак
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO file_groups (user_id, folder_id, group_name) VALUES (?,?,?)",
            (message.from_user.id, folder_id, name)
        )
        group_id = cur.lastrowid
        # Переносим файл в новый пак
        await db.execute("UPDATE files SET group_id=? WHERE id=? AND user_id=?",
                         (group_id, file_db_id, message.from_user.id))
        await db.commit()

    f = await file_get(file_db_id)
    if not f:
        await message.answer(f"{pe('cross','❌')} Файл не найден.", reply_to_message_id=message.message_id)
        return
    tags = await file_get_tags(file_db_id)
    await message.answer(
        f"{pe('check','✅')} Файл добавлен в новый пак «{name}».",
        reply_markup=kb_file(file_db_id, folder_id, bool(f[7]), tags, group_id),
        reply_to_message_id=message.message_id
    )

@dp.callback_query(F.data.startswith("delete_group:"))
async def cb_confirm_delete_group(callback: CallbackQuery):
    group_id = int(callback.data.split(":")[1])
    info = await group_get(group_id)
    if not info or info["user_id"] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True)
        return
    await callback.message.edit_text(
        f"{pe('trash','🗑')} <b>Удалить пак?</b>\n\n"
        f"Пак «{info['group_name']}» ({info['files_count']} файлов) будет расформирован, файлы останутся в папке.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Да, удалить", callback_data=f"confirm_delete_group:{group_id}",
                                     icon_custom_emoji_id=pei("check")),
                InlineKeyboardButton(text="Отмена", callback_data=f"group_view:{group_id}",
                                     icon_custom_emoji_id=pei("cross"))
            ]
        ])
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("confirm_delete_group:"))
async def cb_delete_group(callback: CallbackQuery):
    group_id = int(callback.data.split(":")[1])
    info = await group_get(group_id)
    if not info or info["user_id"] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True)
        return
    folder_id = info["folder_id"]
    await group_delete(group_id, callback.from_user.id)
    await callback.message.edit_text(
        f"{pe('check','✅')} <b>Пак удалён.</b> Файлы остались в папке.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Вернуться в папку", callback_data=f"folder:{folder_id}:0",
                                  icon_custom_emoji_id=pei("back"))]
        ])
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("add_to_group:"))
async def cb_start_add_to_group(callback: CallbackQuery):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s)
    folder_id = int(folder_id_s)
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Файл не найден", show_alert=True)
        return
    groups = await folder_groups_list(folder_id, callback.from_user.id)
    if not groups:
        # Нет паков – сразу предложить создать
        await callback.message.edit_text(
            f"{pe('archive','🗂')} В этой папке пока нет паков.\nСоздать новый пак?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="Создать пак",
                    callback_data=f"create_new_pack_for_file:{file_db_id}:{folder_id}:add",
                    icon_custom_emoji_id=pei("plus")
                )],
                [InlineKeyboardButton(
                    text="Отмена",
                    callback_data=f"file:{file_db_id}:{folder_id}",
                    icon_custom_emoji_id=pei("cross")
                )]
            ])
        )
        await callback.answer()
        return
    # Паки есть – показываем список + кнопку создать
    rows = []
    for gid, gname in groups:
        rows.append([InlineKeyboardButton(
            text=gname,
            callback_data=f"add_to_group_confirm:{file_db_id}:{gid}",
            icon_custom_emoji_id=pei("archive")
        )])
    rows.append([InlineKeyboardButton(
        text="Создать новый пак",
        callback_data=f"create_new_pack_for_file:{file_db_id}:{folder_id}:add",
        icon_custom_emoji_id=pei("plus")
    )])
    rows.append([InlineKeyboardButton(
        text="Отмена",
        callback_data=f"file:{file_db_id}:{folder_id}",
        icon_custom_emoji_id=pei("cross")
    )])
    await callback.message.edit_text(
        f"{pe('plus','➕')} Добавить «{f[3]}» в пак.\nВыберите пак:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("move_to_pack_start:"))
async def cb_start_move_to_pack(callback: CallbackQuery):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s)
    folder_id = int(folder_id_s)
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Файл не найден", show_alert=True)
        return
    groups = await folder_groups_list(folder_id, callback.from_user.id)
    if not groups:
        await callback.message.edit_text(
            f"{pe('archive','🗂')} В этой папке нет паков.\nСоздать новый пак?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="Создать пак",
                    callback_data=f"create_new_pack_for_file:{file_db_id}:{folder_id}:move",
                    icon_custom_emoji_id=pei("plus")
                )],
                [InlineKeyboardButton(
                    text="Отмена",
                    callback_data=f"file:{file_db_id}:{folder_id}",
                    icon_custom_emoji_id=pei("cross")
                )]
            ])
        )
        await callback.answer()
        return
    rows = []
    for gid, gname in groups:
        rows.append([InlineKeyboardButton(
            text=gname,
            callback_data=f"move_to_pack_confirm:{file_db_id}:{gid}:{folder_id}",
            icon_custom_emoji_id=pei("archive")
        )])
    rows.append([InlineKeyboardButton(
        text="Создать новый пак",
        callback_data=f"create_new_pack_for_file:{file_db_id}:{folder_id}:move",
        icon_custom_emoji_id=pei("plus")
    )])
    rows.append([InlineKeyboardButton(
        text="Отмена",
        callback_data=f"file:{file_db_id}:{folder_id}",
        icon_custom_emoji_id=pei("cross")
    )])
    await callback.message.edit_text(
        f"{pe('move','↔️')} Переместить «{f[3]}» в другой пак.\nВыберите пак:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("create_new_pack_for_file:"))
async def cb_create_new_pack_for_file(callback: CallbackQuery, state: FSMContext):
    _, file_db_id_s, folder_id_s, action = callback.data.split(":")
    file_db_id = int(file_db_id_s)
    folder_id = int(folder_id_s)
    folder = await folder_get(folder_id)
    if not folder or folder[1] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True)
        return
    f = await file_get(file_db_id)
    if not f:
        await callback.answer(f"{pe('cross','❌')} Файл не найден", show_alert=True)
        return
    await state.set_state(States.creating_pack_quick)
    await state.update_data(
        pack_action=action,  # "add" или "move"
        pack_file_db_id=file_db_id,
        pack_folder_id=folder_id
    )
    await callback.message.edit_text(
        f"{pe('pencil','✏️')} <b>Название нового пака</b>\n\nВведите название:",
        reply_markup=kb_cancel(f"file:{file_db_id}:{folder_id}")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("remove_from_group:"))
async def cb_remove_from_group(callback: CallbackQuery):
    _, file_db_id_s, folder_id_s = callback.data.split(":")
    file_db_id = int(file_db_id_s)
    folder_id = int(folder_id_s)
    f = await file_get(file_db_id)
    if not f or f[6] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Нет доступа", show_alert=True)
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE files SET group_id=NULL WHERE id=? AND user_id=?",
                         (file_db_id, callback.from_user.id))
        await db.commit()
    tags = await file_get_tags(file_db_id)
    f2 = await file_get(file_db_id)
    await callback.message.edit_text(
        f"{file_emoji_pe(f2[2])} <b>{f2[3]}</b>\n\n"
        f"{pe('doc','📄')} Тип: <b>{file_type_name(f2[2])}</b>\n"
        f"{pe('box','📦')} Размер: <b>{format_size(f2[4])}</b>\n"
        f"{pe('tag','🏷')} Теги: {'  '.join(f'<code>#{t}</code>' for t in tags) if tags else 'нет'}"
        + (f"\n\n{pe('write','📝')} <i>{f2[8]}</i>" if f2[8] else ""),
        reply_markup=kb_file(file_db_id, folder_id, bool(f2[7]), tags, None)
    )
    await callback.answer("✅ Убрано из пака")

async def group_delete_file(file_db_id: int, user_id: int):
    """Полностью удалить файл из пака и из хранилища."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Удаляем файл из БД
        await db.execute("DELETE FROM files WHERE id=? AND user_id=?", (file_db_id, user_id))
        await db.commit()

# admin panel (unchanged)
@dp.callback_query(F.data == "admin_panel")
async def cb_admin_panel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if callback.from_user.id != ADMIN_ID: await callback.answer(f"{pe('cross','❌')} Доступ запрещён", show_alert=True); return
    await callback.message.edit_text(f"{pe('crown','👑')} <b>Панель администратора</b>\n\nВыберите действие:", reply_markup=kb_admin_main())
    await callback.answer()

# --- НАЧАЛО ОБЪЕДИНЕНИЯ ПАКОВ ---
@dp.callback_query(F.data.startswith("merge_groups_start:"))
async def cb_merge_groups_start(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split(":")[1])
    folder = await folder_get(folder_id)
    if not folder or folder[1] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    groups = await folder_groups_list(folder_id, callback.from_user.id)
    if len(groups) < 2:
        await callback.answer("В папке недостаточно паков для объединения (нужно хотя бы два).", show_alert=True)
        return

    await state.set_state(States.selecting_groups_merge)
    await state.update_data(folder_id=folder_id, selected_groups=set())
    await callback.message.edit_text(
        f"{pe('archive','🗂')} <b>Объединение паков</b>\n\n"
        f"{pe('folder','📁')} Папка: <b>{folder[2]}</b>\n"
        "Выберите два или более пака для объединения:",
        reply_markup=kb_merge_select(groups, set(), folder_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("merge_select:"), States.selecting_groups_merge)
async def cb_merge_select_group(callback: CallbackQuery, state: FSMContext):
    group_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    selected = set(data.get("selected_groups", []))
    folder_id = data["folder_id"]

    if group_id in selected:
        selected.remove(group_id)
    else:
        selected.add(group_id)

    await state.update_data(selected_groups=selected)

    # Обновляем клавиатуру
    groups = await folder_groups_list(folder_id, callback.from_user.id)
    await callback.message.edit_reply_markup(
        reply_markup=kb_merge_select(groups, selected, folder_id)
    )
    await callback.answer()

@dp.callback_query(F.data == "merge_confirm", States.selecting_groups_merge)
async def cb_merge_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = data.get("selected_groups", set())
    if len(selected) < 2:
        await callback.answer("Выберите минимум два пака!", show_alert=True)
        return

    await state.set_state(States.naming_merged_group)
    await state.update_data(selected_groups=selected)

    await callback.message.edit_text(
        f"{pe('pencil','✏️')} <b>Имя для нового объединённого пака</b>\n\n"
        "Введите название:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data=f"merge_cancel:{data['folder_id']}",
                                  icon_custom_emoji_id=pei("cross"))]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("merge_cancel:"), States.selecting_groups_merge)
async def cb_merge_cancel(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split(":")[1])
    await state.clear()
    await _show_folder(callback, folder_id, 0, state=state)  # возврат в папку
    await callback.answer("Отменено")

@dp.message(States.naming_merged_group, F.text)
async def msg_merged_group_name(message: Message, state: FSMContext):
    new_name = message.text.strip()
    if len(new_name) > 50:
        await message.answer(
            f"{pe('cross','❌')} Слишком длинное название (макс. 50 символов).",
            reply_to_message_id=message.message_id
        )
        return

    data = await state.get_data()
    selected = list(data["selected_groups"])
    folder_id = data["folder_id"]
    user_id = message.from_user.id

    try:
        new_group_id = await group_merge(user_id, folder_id, selected, new_name)
    except ValueError as e:
        await message.answer(
            f"{pe('cross','❌')} Ошибка: {e}",
            reply_to_message_id=message.message_id
        )
        await state.clear()
        return

    await state.clear()

    # Мгновенно показываем папку с новым объединённым паком
    await _show_folder_by_message(message, folder_id)

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: await callback.answer(f"{pe('cross','❌')} Доступ запрещён", show_alert=True); return
    s = await admin_stats(); recent = await admin_get_recent_users(5)
    recent_text = "\n".join(f"  {pe('profile','👤')} <b>{u[2] or 'Без имени'}</b>" + (f" @{u[1]}" if u[1] else "") + f" — <code>{u[0]}</code>" for u in recent) or "  —"
    top_text = "\n".join(f"  {file_emoji_pe(ft)} {file_type_name(ft)}: <b>{cnt}</b>" for ft, cnt in s["top_types"]) or "  —"
    await callback.message.edit_text(
        f"{pe('stats','📊')} <b>Статистика бота</b>\n\n"
        f"{pe('profile','👤')} Пользователей: <b>{s['users']}</b>\n{pe('crown','👑')} Premium: <b>{s['premium']}</b>\n"
        f"{pe('folder','📁')} Папок: <b>{s['folders']}</b>\n{pe('doc','📄')} Файлов: <b>{s['files']}</b>\n"
        f"{pe('box','📦')} Объём: <b>{format_size(s['size'])}</b>\n{pe('people','👥')} Рефералов: <b>{s['refs']}</b>\n\n"
        f"<b>Топ типов файлов:</b>\n{top_text}\n\n<b>Новые пользователи:</b>\n{recent_text}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄  Обновить", callback_data="admin_stats", icon_custom_emoji_id=pei("copy"))],
                                                          [InlineKeyboardButton(text="◁  Назад", callback_data="admin_panel", icon_custom_emoji_id=pei("back"))]]))
    await callback.answer()

# (other admin handlers unchanged, already present above; same for link upload, etc.)
@dp.message(F.text == "Загрузить по ссылке")
async def cmd_link_upload(message: Message, state: FSMContext):
    await state.set_state(States.waiting_url)
    await message.answer(f"{pe('link','🔗')} Отправьте ссылку на файл (http:// или https://):", reply_markup=reply_cancel())

@dp.message(States.waiting_url, F.text)
async def process_url(message: Message, state: FSMContext):
    url = message.text.strip()
    if not url.startswith(("http://", "https://")):
        await message.answer(f"{pe('cross','❌')} Неверный URL")
        return
    is_youtube = "youtube.com" in url or "youtu.be" in url
    if is_youtube:
        await message.answer(
            f"{pe('cross','❌')} YouTube временно недоступен для скачивания.\n\n"
            "Вы можете:\n"
            "• Скачать видео вручную и отправить файл\n"
            "• Использовать прямую ссылку на файл (не YouTube)"
        )
        return
    await message.answer(f"{pe('download','⬇️')} Скачиваю файл...")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status != 200:
                    await message.answer(f"{pe('cross','❌')} Ошибка загрузки")
                    return
                file_bytes = await resp.read()
                file_name = url.split("/")[-1] or "file"
                file_size = len(file_bytes)
                if file_name.lower().endswith(('.mp4', '.mkv', '.avi', '.mov', '.webm')):
                    file_type = "video"
                elif file_name.lower().endswith(('.mp3', '.wav', '.ogg', '.m4a', '.flac')):
                    file_type = "audio"
                elif file_name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp')):
                    file_type = "photo"
                else:
                    file_type = "document"
        except asyncio.TimeoutError:
            await message.answer(f"{pe('cross','❌')} Превышено время ожидания")
            return
    content_length = resp.headers.get("Content-Length")
    if content_length and int(content_length) > 50 * 1024 * 1024:
        await message.answer(f"{pe('cross','❌')} Файл слишком большой (лимит 50 МБ для прямой загрузки).")
        return
    await state.update_data(pending_file=(file_bytes, file_type, file_name, file_size))
    await state.set_state(States.select_folder_for_url)
    folders, total = await folders_get(message.from_user.id, 0)
    rows = []
    for fid, name, is_public, _, _, _ in folders:
        rows.append([InlineKeyboardButton(
            text=name,
            callback_data=f"url_save_folder:{fid}",
            icon_custom_emoji_id=pei("folder"),
        )])
    rows.append([InlineKeyboardButton(text="Отмена", callback_data="main_menu", icon_custom_emoji_id=pei("cross"))])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await message.answer(f"{pe('folder','📁')} Выберите папку для сохранения:", reply_markup=kb)

@dp.callback_query(F.data.startswith("url_save_folder:"))
async def cb_save_url_file(callback: CallbackQuery, state: FSMContext):
    folder_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    pending = data.get("pending_file")
    if not pending:
        await callback.answer("Ошибка", show_alert=True)
        return
    file_bytes, file_type, file_name, file_size = pending
    document = BufferedInputFile(file_bytes, filename=file_name)
    msg = await callback.message.answer_document(document)
    file_id = msg.document.file_id
    await file_save(callback.from_user.id, folder_id, file_id, file_type, file_name, file_size)
    await state.clear()
    await callback.message.edit_text(f"{pe('check','✅')} Файл сохранён!")
    await callback.answer()

@dp.callback_query(F.data.startswith("upload_to_pack:"))
async def cb_upload_to_pack(callback: CallbackQuery, state: FSMContext):
    group_id = int(callback.data.split(":")[1])
    info = await group_get(group_id)
    if not info or info["user_id"] != callback.from_user.id:
        await callback.answer(f"{pe('cross','❌')} Пак не найден", show_alert=True)
        return
    await state.set_state(States.uploading)
    await state.update_data(folder_id=info["folder_id"], upload_group_id=group_id)
    await callback.message.edit_text(
        f"{pe('upload','⬆️')} <b>Добавление в пак: {info['group_name']}</b>\n\n"
        f"{pe('info','ℹ')} Отправляйте файлы — они будут добавлены в этот пак.\n"
        f"{pe('check','✅')} Нажми <b>Готово</b>, чтобы вернуться в пак.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Готово — вернуться в пак", callback_data=f"return_to_pack:{group_id}",
                                  icon_custom_emoji_id=pei("check"))]
        ])
    )
    await callback.answer()

def kb_merge_select(groups: list[tuple[int, str]], selected: set[int], folder_id: int):
    rows = []
    for gid, gname in groups:
        # Помечаем выбранные иконкой/галочкой
        mark = pei("check2") if gid in selected else ""
        text = f"{gname} {'✅' if gid in selected else ''}"
        rows.append([
            InlineKeyboardButton(
                text=text,
                callback_data=f"merge_select:{gid}",
                icon_custom_emoji_id=pei("archive") if not mark else pei("check2")
            )
        ])
    # Кнопка подтверждения (активна только при выборе >=2)
    if len(selected) >= 2:
        rows.append([
            InlineKeyboardButton(
                text="Объединить выбранное",
                callback_data="merge_confirm",
                icon_custom_emoji_id=pei("check")
            )
        ])
    rows.append([
        InlineKeyboardButton(
            text="Отмена",
            callback_data=f"merge_cancel:{folder_id}",
            icon_custom_emoji_id=pei("cross")
        )
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data.startswith("return_to_pack:"))
async def cb_return_to_pack(callback: CallbackQuery, state: FSMContext):
    group_id = int(callback.data.split(":")[1])
    await state.clear()
    info = await group_get(group_id)
    if not info:
        await callback.answer(f"{pe('cross','❌')} Пак не найден", show_alert=True)
        return
    files = await group_files_list(group_id, 0)
    text = (
        f"{pe('archive','🗂')} <b>Пак: {info['group_name']}</b>\n\n"
        f"{pe('doc','📄')} Файлов: <b>{info['files_count']}</b>  "
        f"{pe('box','📦')} <b>{format_size(info['total_size'])}</b>"
    )
    await callback.message.edit_text(text, reply_markup=kb_group_view(group_id, files, info))
    await callback.answer()



# Entry point
async def main():
    await db_init()
    logger.info("Bot started. Polling…")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())