import os
import hmac
import hashlib
import logging
import json
import aiohttp
import asyncio 
import aiosqlite
from urllib.parse import parse_qs, unquote
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Optional
from aiogram.types import BufferedInputFile
from aiogram import Bot
import io

# Загружаем .env
load_dotenv(Path(__file__).parent / ".env")

# ──────────────────────────────────────────── Config
DB_PATH = os.getenv("DB_PATH", "storage.db")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
PORT = int(os.getenv("WEBAPP_PORT", "8000"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

# ──────────────────────────────────────────── Security
def verify_init_data(init_data: str) -> dict:
    """Проверяет подпись Telegram WebApp initData"""
    parsed = parse_qs(init_data)
    hash_str = parsed.pop('hash', [None])[0]
    if not hash_str:
        return {}
    data_check_string = "\n".join(
        f"{k}={unquote(v[0])}" for k, v in sorted(parsed.items())
    )
    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    calc_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if hmac.compare_digest(calc_hash, hash_str):
        return {k: unquote(v[0]) for k, v in parsed.items()}
    return {}

def get_user_id(request: Request) -> int:
    init_data = request.headers.get("X-Telegram-Init-Data", "").strip()
    
    if not init_data:
        dev_uid = request.query_params.get("dev_user_id")
        if dev_uid and dev_uid.isdigit():
            return int(dev_uid)
        raise HTTPException(401, "Missing initData. Откройте через Telegram.")
    
    data = verify_init_data(init_data)
    if not data or "user" not in data:
        raise HTTPException(401, "Invalid initData signature.")
    
    user_info = json.loads(data["user"])
    return int(user_info["id"])

# ──────────────────────────────────────────── Models
class RenameRequest(BaseModel):
    file_id: int
    new_name: str

class ActionRequest(BaseModel):
    action: str
    file_ids: List[int]

# ──────────────────────────────────────────── Endpoints
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/folders")
async def get_folders(user_id: int = Depends(get_user_id), parent_id: Optional[int] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        if parent_id is None:
            clause = "parent_id IS NULL"
            params = (user_id,)
        else:
            clause = "parent_id = ?"
            params = (user_id, parent_id)
        async with db.execute(
            f"SELECT id, name, is_public FROM folders WHERE user_id = ? AND {clause} ORDER BY name",
            params
        ) as cur:
            rows = await cur.fetchall()
    return {"folders": [{"id": r[0], "name": r[1], "is_public": bool(r[2])} for r in rows]}

@app.get("/api/files/{folder_id}")
async def get_files(folder_id: int, user_id: int = Depends(get_user_id)):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, file_name, file_type, file_size, file_id FROM files WHERE folder_id = ? AND user_id = ? ORDER BY created_at DESC LIMIT 200",
            (folder_id, user_id)
        ) as cur:
            rows = await cur.fetchall()
    return {"files": [{"id": r[0], "name": r[1], "type": r[2], "size": r[3], "file_id": r[4]} for r in rows]}

@app.get("/api/download/{file_id}")
async def download_file(file_id: int, user_id: int = Depends(get_user_id)):
    """Возвращает прямую ссылку для скачивания из Telegram"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT file_id, file_type, file_name FROM files WHERE id = ? AND user_id = ?",
                (file_id, user_id)
            ) as cur:
                row = await cur.fetchone()
        
        if not row:
            logger.warning(f"File not found: {file_id} for user {user_id}")
            raise HTTPException(404, "File not found")
        
        tg_file_id, file_type, file_name = row
        
        logger.info(f"Getting download URL for: {file_name} ({tg_file_id})")
        
        # Запрашиваем информацию о файле у Telegram
        tg_file_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={tg_file_id}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(tg_file_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.error(f"Telegram API error: {resp.status}")
                    raise HTTPException(500, "Failed to get file from Telegram")
                
                data = await resp.json()
                
                if not data.get("ok"):
                    logger.error(f"Telegram error: {data}")
                    raise HTTPException(500, "Telegram API error")
                
                file_path = data["result"]["file_path"]
                download_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
                
                logger.info(f"✅ Got download URL")
                
                return {
                    "url": download_url,
                    "file_type": file_type,
                    "file_name": file_name,
                    "file_id": tg_file_id
                }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Download error: {e}")
        raise HTTPException(500, f"Error: {str(e)}")

@app.post("/api/rename")
async def rename_file(req: RenameRequest, user_id: int = Depends(get_user_id)):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE files SET file_name = ? WHERE id = ? AND user_id = ?",
            (req.new_name, req.file_id, user_id)
        )
        await db.commit()
    return {"status": "ok"}

@app.post("/api/action")
async def perform_action(req: ActionRequest, user_id: int = Depends(get_user_id)):
    if req.action != "delete":
        raise HTTPException(400, "Unsupported action")
    async with aiosqlite.connect(DB_PATH) as db:
        placeholders = ",".join("?" for _ in req.file_ids)
        await db.execute(
            f"DELETE FROM files WHERE id IN ({placeholders}) AND user_id = ?",
            [*req.file_ids, user_id]
        )
        await db.commit()
    return {"status": "ok"}

class CreateFolderRequest(BaseModel):
    name: str
    is_public: bool = False

@app.post("/api/folders")
async def create_folder(req: CreateFolderRequest, user_id: int = Depends(get_user_id)):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT INTO folders (user_id, name, is_public) VALUES (?, ?, ?)",
            (user_id, req.name, req.is_public)
        )
        await db.commit()
        folder_id = cursor.lastrowid
    return {"status": "ok", "folder_id": folder_id}
    
def format_size(size: int) -> str:
    """Форматирует размер файла"""
    if size < 1024:
        return f"{size} B"
    elif size < 1024**2:
        return f"{size/1024:.1f} KB"
    elif size < 1024**3:
        return f"{size/1024**2:.1f} MB"
    else:
        return f"{size/1024**3:.2f} GB"
    
@app.get("/favicon.ico")
async def favicon():
    return {"status": "ok"}

@app.get("/api/files/{folder_id}")
async def get_files(
    folder_id: int,
    user_id: int = Depends(get_user_id),
    page: int = 1,
    per_page: int = 20,
    sort: str = "created_at",
    order: str = "desc",
    file_type: Optional[str] = None
):
    """Возвращает файлы папки с пагинацией, сортировкой и фильтром."""
    offset = (page - 1) * per_page
    allowed_sorts = {"name": "file_name", "size": "file_size", "created_at": "created_at"}
    sort_col = allowed_sorts.get(sort, "created_at")
    order_direction = "ASC" if order.lower() == "asc" else "DESC"
    
    async with aiosqlite.connect(DB_PATH) as db:
        conditions = ["folder_id = ?", "user_id = ?"]
        params = [folder_id, user_id]
        if file_type:
            conditions.append("file_type = ?")
            params.append(file_type)
        where = " AND ".join(conditions)
        
        async with db.execute(f"SELECT COUNT(*) FROM files WHERE {where}", params) as cur:
            total = (await cur.fetchone())[0]
        
        async with db.execute(
            f"SELECT id, file_id, file_type, file_name, file_size, created_at, group_id FROM files WHERE {where} "
            f"ORDER BY {sort_col} {order_direction} LIMIT ? OFFSET ?",
            [*params, per_page, offset]
        ) as cur:
            rows = await cur.fetchall()
    
    return {
        "files": [{"id": r[0], "file_id": r[1], "type": r[2], "name": r[3], "size": r[4], "created": r[5], "group_id": r[6]} for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page
    }

@app.get("/api/search")
async def search_files(
    q: str = "",
    user_id: int = Depends(get_user_id),
    folder_id: Optional[int] = None,
    file_type: Optional[str] = None,
    page: int = 1,
    per_page: int = 20
):
    """Поиск по файлам с фильтрами."""
    if not q and not file_type:
        return {"files": [], "total": 0}
    
    offset = (page - 1) * per_page
    conditions = ["f.user_id = ?"]
    params = [user_id]
    if q:
        conditions.append("f.file_name LIKE ?")
        params.append(f"%{q}%")
    if folder_id:
        conditions.append("f.folder_id = ?")
        params.append(folder_id)
    if file_type:
        conditions.append("f.file_type = ?")
        params.append(file_type)
    where = " AND ".join(conditions)
    
    async with aiosqlite.connect(DB_PATH) as db:
        query = f"""
            SELECT f.id, f.file_id, f.file_type, f.file_name, f.file_size, f.created_at, f.folder_id,
                   fo.name as folder_name
            FROM files f LEFT JOIN folders fo ON fo.id = f.folder_id
            WHERE {where}
            ORDER BY f.created_at DESC LIMIT ? OFFSET ?
        """
        async with db.execute(query, [*params, per_page, offset]) as cur:
            rows = await cur.fetchall()
        async with db.execute(f"SELECT COUNT(*) FROM files f WHERE {where}", params) as cur:
            total = (await cur.fetchone())[0]
    return {
        "files": [{"id": r[0], "file_id": r[1], "type": r[2], "name": r[3], "size": r[4], "created": r[5], "folder_id": r[6], "folder_name": r[7]} for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page
    }

@app.get("/api/folders")
async def get_folders(parent_id: Optional[int] = None, user_id: int = Depends(get_user_id)):
    async with aiosqlite.connect(DB_PATH) as db:
        if parent_id is None:
            clause = "parent_id IS NULL"
            params = (user_id,)
        else:
            clause = f"parent_id = {parent_id}"
            params = (user_id,)
        async with db.execute(
            f"SELECT id, name, is_public FROM folders WHERE user_id = ? AND {clause} ORDER BY name",
            params
        ) as cur:
            rows = await cur.fetchall()
    return {"folders": [{"id": r[0], "name": r[1], "is_public": bool(r[2])} for r in rows]}

@app.post("/api/upload")
async def upload_file(
    request: Request,
    user_id: int = Depends(get_user_id)
):
    """Загружает файл в Telegram и сохраняет file_id в БД"""
    try:
        form_data = await request.form()
        folder_id_str = form_data.get("folder_id", "0")
        folder_name = form_data.get("folder_name", "Папка")
        file_obj = form_data.get("file")

        if not file_obj:
            raise HTTPException(400, "Файл не найден в запросе")

        try:
            folder_id = int(folder_id_str)
        except:
            raise HTTPException(400, "Некорректный folder_id")

        # Проверка доступа к папке
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id FROM folders WHERE id = ? AND user_id = ?",
                (folder_id, user_id)
            ) as cur:
                if not await cur.fetchone():
                    raise HTTPException(403, "Папка не найдена или нет доступа")

        # Читаем файл
        file_content = await file_obj.read()
        file_name = file_obj.filename or "file"
        file_size = len(file_content)

        logger.info(f"📤 Загрузка файла: {file_name} ({format_size(file_size)})")

        # Определяем тип файла
        file_type = "document"
        content_type = file_obj.content_type or ""
        file_ext = file_name.split('.')[-1].lower() if '.' in file_name else ''

        if content_type.startswith("image/") or file_ext in ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp']:
            file_type = "photo"
        elif content_type.startswith("video/") or file_ext in ['mp4', 'avi', 'mov', 'mkv', 'webm', 'flv']:
            file_type = "video"
        elif content_type.startswith("audio/") or file_ext in ['mp3', 'wav', 'ogg', 'm4a', 'flac', 'aac']:
            file_type = "audio"

        logger.info(f"📋 Тип: {file_type}")

        # Создаём экземпляр бота
        bot_instance = Bot(token=BOT_TOKEN)
        caption = f"Папка: {folder_name}"

        try:
            buffered_file = BufferedInputFile(file_content, filename=file_name)
            logger.info("📨 Отправка в Telegram...")

            # ВАЖНО: request_timeout = 600 (10 минут) — решает проблему обрыва
            if file_type == "photo":
                message = await bot_instance.send_photo(
                    chat_id=user_id,
                    photo=buffered_file,
                    caption=caption,
                    request_timeout=600
                )
                file_id = message.photo[-1].file_id

            elif file_type == "video":
                message = await bot_instance.send_video(
                    chat_id=user_id,
                    video=buffered_file,
                    caption=caption,
                    request_timeout=600
                )
                file_id = message.video.file_id

            elif file_type == "audio":
                message = await bot_instance.send_audio(
                    chat_id=user_id,
                    audio=buffered_file,
                    title=file_name,
                    caption=caption,
                    request_timeout=600
                )
                file_id = message.audio.file_id

            else:
                message = await bot_instance.send_document(
                    chat_id=user_id,
                    document=buffered_file,
                    caption=caption,
                    request_timeout=600
                )
                file_id = message.document.file_id

            logger.info(f"✅ Файл загружен в Telegram: {file_id}")

        except asyncio.CancelledError:
            logger.error("Загрузка прервана (таймаут или обрыв соединения)")
            raise HTTPException(504, "Превышено время ожидания. Попробуйте ещё раз.")
        except Exception as e:
            logger.error(f"Ошибка Telegram API: {e}")
            raise HTTPException(500, f"Ошибка отправки в Telegram: {str(e)}")
        finally:
            await bot_instance.session.close()

        # Сохраняем file_id в базу данных
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "INSERT INTO files (user_id, folder_id, file_id, file_type, file_name, file_size) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, folder_id, file_id, file_type, file_name, file_size)
            )
            await db.commit()
            db_id = cursor.lastrowid

        logger.info(f"💾 Запись в БД: ID={db_id}, FILE_ID={file_id}")
        return {
            "status": "ok",
            "message": f"Файл '{file_name}' загружен",
            "id": db_id
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        raise HTTPException(500, f"Внутренняя ошибка: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=True)