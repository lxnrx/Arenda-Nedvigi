import asyncio
import logging
import os
import signal
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import asyncpg
from datetime import datetime
from typing import Optional
import secrets

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Токен бота
BOT_TOKEN = os.getenv("BOT_TOKEN", "8232122685:AAGvUGaYfQPkrX0l107UogvGz6n6yWJ_OQs")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:lGmnVeXVJlsynNhcfVhrsYBValEzJQvl@postgres.railway.internal:5432/railway")

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Глобальный пул соединений
db_pool: Optional[asyncpg.Pool] = None

# Helper функция для безопасной очистки state
async def clear_state_keep_company(state: FSMContext):
    """Очищает state, но сохраняет current_company_id"""
    data = await state.get_data()
    company_id = data.get('current_company_id')
    await state.clear()
    if company_id:
        await state.update_data(current_company_id=company_id)

# Allowlist для безопасных полей компании (защита от SQL injection)
ALLOWED_COMPANY_FIELDS = {
    'name', 'city', 'welcome_message', 'timezone_offset', 
    'checkin_time', 'checkout_time', 'long_term_only'
}

# Инициализация базы данных
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    
    async with db_pool.acquire() as conn:
        # Таблица пользователей
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                first_start BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Таблица компаний
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                city TEXT NOT NULL,
                welcome_message TEXT,
                timezone_offset INTEGER DEFAULT 0,
                checkin_time TEXT DEFAULT '14:00',
                checkout_time TEXT DEFAULT '12:00',
                long_term_only BOOLEAN DEFAULT FALSE,
                invite_code TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Таблица связи пользователей и компаний
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_companies (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                company_id INTEGER REFERENCES companies(id),
                is_admin BOOLEAN DEFAULT FALSE,
                UNIQUE(user_id, company_id)
            )
        ''')
        
        # Таблица объектов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS properties (
                id SERIAL PRIMARY KEY,
                company_id INTEGER REFERENCES companies(id),
                name TEXT NOT NULL,
                address TEXT,
                is_short_term BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Таблица информации по объектам
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS property_info (
                id SERIAL PRIMARY KEY,
                property_id INTEGER REFERENCES properties(id) ON DELETE CASCADE,
                section TEXT NOT NULL,
                field_key TEXT NOT NULL,
                field_name TEXT NOT NULL,
                text_content TEXT,
                file_id TEXT,
                file_type TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(property_id, section, field_key)
            )
        ''')
        
        # Таблица бронирований
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bookings (
                id SERIAL PRIMARY KEY,
                property_id INTEGER REFERENCES properties(id) ON DELETE CASCADE,
                guest_name TEXT NOT NULL,
                checkin_date DATE NOT NULL,
                checkout_date DATE,
                access_code TEXT UNIQUE NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Таблица админов бота
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bot_admins (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE NOT NULL,
                username TEXT,
                first_name TEXT,
                added_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Таблица предложений по улучшению бота
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS suggestions (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                suggestion_text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                is_read BOOLEAN DEFAULT FALSE
            )
        ''')
        
        # МИГРАЦИИ для существующих баз данных
        logger.info("Running database migrations...")
        
        # Миграция 1: Добавляем first_start в users (с проверкой существования)
        try:
            column_exists = await conn.fetchval('''
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'users' AND column_name = 'first_start'
                )
            ''')
            
            if not column_exists:
                await conn.execute('ALTER TABLE users ADD COLUMN first_start BOOLEAN DEFAULT TRUE')
                logger.info("✅ Migration: Added first_start column to users")
            else:
                logger.info("ℹ️  first_start column already exists")
        except Exception as e:
            logger.error(f"❌ first_start migration failed: {e}")
        
        # Миграция 2: Добавляем invite_code в companies (с проверкой существования)
        try:
            column_exists = await conn.fetchval('''
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'companies' AND column_name = 'invite_code'
                )
            ''')
            
            if not column_exists:
                await conn.execute('ALTER TABLE companies ADD COLUMN invite_code TEXT')
                logger.info("✅ Migration: Added invite_code column to companies")
                
                # Создаем уникальный индекс
                await conn.execute('CREATE UNIQUE INDEX companies_invite_code_key ON companies(invite_code)')
                logger.info("✅ Migration: Created unique index for invite_code")
            else:
                logger.info("ℹ️  invite_code column already exists")
        except Exception as e:
            logger.error(f"❌ invite_code migration failed: {e}")
        
        # Миграция 3: Генерируем invite_code для существующих компаний
        try:
            result = await conn.execute('''
                UPDATE companies 
                SET invite_code = md5(random()::text || id::text)
                WHERE invite_code IS NULL
            ''')
            if result != 'UPDATE 0':
                logger.info(f"✅ Migration: Generated invite codes ({result})")
        except Exception as e:
            logger.error(f"❌ generate invite_code migration failed: {e}")
        
        logger.info("Database initialized successfully")

# Состояния FSM
class CompanyStates(StatesGroup):
    waiting_company_name = State()
    waiting_company_city = State()
    editing_company_name = State()
    editing_company_city = State()
    editing_company_welcome = State()
    waiting_timezone = State()
    waiting_checkin_time = State()
    waiting_checkout_time = State()

class PropertyStates(StatesGroup):
    waiting_property_name = State()
    waiting_property_address = State()
    editing_field = State()
    adding_custom_button_name = State()
    adding_custom_button_content = State()
    waiting_custom_confirm = State()
    editing_property_name = State()
    editing_property_address = State()

class BookingStates(StatesGroup):
    waiting_guest_name = State()
    waiting_checkin_date = State()

class SuggestionStates(StatesGroup):
    waiting_suggestion = State()

# Вспомогательные функции БД
async def get_user_companies(user_id: int):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT c.id, c.name, c.city 
            FROM companies c
            JOIN user_companies uc ON c.id = uc.company_id
            WHERE uc.user_id = $1
        ''', user_id)
        return [(row['id'], row['name'], row['city']) for row in rows]

async def create_company(name: str, city: str, user_id: int):
    welcome_msg = "Добрый день! Добро пожаловать! Вы находитесь в боте-помощнике для ваших апартаментов."
    invite_code = secrets.token_urlsafe(16)
    
    async with db_pool.acquire() as conn:
        company_id = await conn.fetchval('''
            INSERT INTO companies (name, city, welcome_message, invite_code)
            VALUES ($1, $2, $3, $4)
            RETURNING id
        ''', name, city, welcome_msg, invite_code)
        
        await conn.execute('''
            INSERT INTO user_companies (user_id, company_id, is_admin)
            VALUES ($1, $2, TRUE)
        ''', user_id, company_id)
        
        return company_id

async def get_company_info(company_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow('''
            SELECT id, name, city, welcome_message, timezone_offset, 
                   checkin_time, checkout_time, long_term_only, invite_code
            FROM companies 
            WHERE id = $1
        ''', company_id)

async def update_company_field(company_id: int, field: str, value):
    if field not in ALLOWED_COMPANY_FIELDS:
        raise ValueError(f"Invalid field: {field}")
    
    async with db_pool.acquire() as conn:
        # Безопасное обновление с использованием параметризованного запроса
        query = f"UPDATE companies SET {field} = $1 WHERE id = $2"
        await conn.execute(query, value, company_id)

async def join_company_by_invite(user_id: int, invite_code: str):
    async with db_pool.acquire() as conn:
        company_id = await conn.fetchval(
            'SELECT id FROM companies WHERE invite_code = $1',
            invite_code
        )
        
        if not company_id:
            return None
        
        # Проверяем, не состоит ли уже
        exists = await conn.fetchval('''
            SELECT 1 FROM user_companies 
            WHERE user_id = $1 AND company_id = $2
        ''', user_id, company_id)
        
        if not exists:
            await conn.execute('''
                INSERT INTO user_companies (user_id, company_id, is_admin)
                VALUES ($1, $2, FALSE)
            ''', user_id, company_id)
        
        return company_id

async def get_company_properties(company_id: int):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT id, name, address, is_short_term 
            FROM properties 
            WHERE company_id = $1
        ''', company_id)
        return [(row['id'], row['name'], row['address'], row['is_short_term']) for row in rows]

async def create_property(company_id: int, name: str, address: str):
    async with db_pool.acquire() as conn:
        property_id = await conn.fetchval('''
            INSERT INTO properties (company_id, name, address)
            VALUES ($1, $2, $3)
            RETURNING id
        ''', company_id, name, address)
        return property_id

async def save_property_field(property_id: int, section: str, field_key: str, 
                             field_name: str, text_content: str = None, 
                             file_id: str = None, file_type: str = None):
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO property_info (property_id, section, field_key, field_name, text_content, file_id, file_type)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (property_id, section, field_key)
            DO UPDATE SET text_content = $5, file_id = $6, file_type = $7
        ''', property_id, section, field_key, field_name, text_content, file_id, file_type)

async def get_property_field(property_id: int, section: str, field_key: str):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow('''
            SELECT text_content, file_id, file_type
            FROM property_info
            WHERE property_id = $1 AND section = $2 AND field_key = $3
        ''', property_id, section, field_key)

async def get_property_sections_data(property_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetch('''
            SELECT section, field_name, text_content, file_id, file_type
            FROM property_info
            WHERE property_id = $1
            ORDER BY section, field_name
        ''', property_id)

async def get_section_fields(property_id: int, section: str):
    async with db_pool.acquire() as conn:
        return await conn.fetch('''
            SELECT field_key, field_name, text_content, file_id, file_type
            FROM property_info
            WHERE property_id = $1 AND section = $2
            ORDER BY field_name
        ''', property_id, section)

async def get_filled_fields(property_id: int, section: str):
    """Получить список заполненных полей раздела"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT field_key
            FROM property_info
            WHERE property_id = $1 AND section = $2
            AND (text_content IS NOT NULL OR file_id IS NOT NULL)
        ''', property_id, section)
        return set(row['field_key'] for row in rows)

async def get_custom_fields(property_id: int, section: str):
    """Получить список кастомных полей раздела"""
    async with db_pool.acquire() as conn:
        return await conn.fetch('''
            SELECT field_key, field_name, text_content, file_id, file_type
            FROM property_info
            WHERE property_id = $1 AND section = $2 AND field_key LIKE 'custom_%'
            ORDER BY created_at
        ''', property_id, section)

async def delete_custom_field(property_id: int, section: str, field_key: str):
    """Удалить кастомное поле"""
    async with db_pool.acquire() as conn:
        await conn.execute('''
            DELETE FROM property_info
            WHERE property_id = $1 AND section = $2 AND field_key = $3
        ''', property_id, section, field_key)

async def create_booking(property_id: int, guest_name: str, checkin_date):
    access_code = secrets.token_urlsafe(32)
    
    if isinstance(checkin_date, str):
        checkin_date = datetime.strptime(checkin_date, '%Y-%m-%d').date()
    
    async with db_pool.acquire() as conn:
        booking_id = await conn.fetchval('''
            INSERT INTO bookings (property_id, guest_name, checkin_date, access_code)
            VALUES ($1, $2, $3, $4)
            RETURNING id
        ''', property_id, guest_name, checkin_date, access_code)
        return booking_id, access_code

async def get_property_bookings(property_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetch('''
            SELECT id, guest_name, checkin_date, checkout_date, access_code, is_active
            FROM bookings
            WHERE property_id = $1 AND is_active = TRUE
            ORDER BY checkin_date DESC
        ''', property_id)

async def get_booking_by_code(access_code: str):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow('''
            SELECT b.id, b.property_id, b.guest_name, b.checkin_date, b.is_active,
                   p.name as property_name, p.address
            FROM bookings b
            JOIN properties p ON b.property_id = p.id
            WHERE b.access_code = $1
        ''', access_code)

async def complete_booking(booking_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute('UPDATE bookings SET is_active = FALSE WHERE id = $1', booking_id)

async def delete_property(property_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute('DELETE FROM properties WHERE id = $1', property_id)

async def toggle_short_term(property_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute('UPDATE properties SET is_short_term = NOT is_short_term WHERE id = $1', property_id)

async def get_property_name(property_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchval('SELECT name FROM properties WHERE id = $1', property_id)

async def get_property_address(property_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchval('SELECT address FROM properties WHERE id = $1', property_id)

async def get_property_info(property_id: int):
    """Получить полную информацию об объекте"""
    async with db_pool.acquire() as conn:
        return await conn.fetchrow('''
            SELECT id, name, address, is_short_term, company_id
            FROM properties
            WHERE id = $1
        ''', property_id)

async def get_company_managers(company_id: int):
    """Получить список менеджеров компании"""
    async with db_pool.acquire() as conn:
        return await conn.fetch('''
            SELECT u.user_id, u.username, u.first_name, uc.is_admin
            FROM user_companies uc
            JOIN users u ON uc.user_id = u.user_id
            WHERE uc.company_id = $1
            ORDER BY uc.is_admin DESC, u.first_name
        ''', company_id)

async def mark_user_not_first_start(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute('UPDATE users SET first_start = FALSE WHERE user_id = $1', user_id)

async def is_first_start(user_id: int):
    async with db_pool.acquire() as conn:
        result = await conn.fetchval('SELECT first_start FROM users WHERE user_id = $1', user_id)
        return result if result is not None else True

# Функции для системы предложений
async def get_bot_admins():
    """Получить список всех админов бота"""
    async with db_pool.acquire() as conn:
        return await conn.fetch('SELECT user_id, username, first_name FROM bot_admins')

async def is_bot_admin(user_id: int):
    """Проверить, является ли пользователь админом бота"""
    async with db_pool.acquire() as conn:
        result = await conn.fetchval('SELECT 1 FROM bot_admins WHERE user_id = $1', user_id)
        return result is not None

async def save_suggestion(user_id: int, username: str, first_name: str, suggestion_text: str):
    """Сохранить предложение в БД"""
    async with db_pool.acquire() as conn:
        suggestion_id = await conn.fetchval('''
            INSERT INTO suggestions (user_id, username, first_name, suggestion_text)
            VALUES ($1, $2, $3, $4)
            RETURNING id
        ''', user_id, username, first_name, suggestion_text)
        return suggestion_id

async def get_recent_suggestions_count(user_id: int, hours: int = 1):
    """Получить количество предложений от пользователя за последние N часов (защита от спама)"""
    async with db_pool.acquire() as conn:
        return await conn.fetchval('''
            SELECT COUNT(*) FROM suggestions
            WHERE user_id = $1 AND created_at > NOW() - INTERVAL '%s hours'
        ''' % hours, user_id)

async def add_bot_admin(user_id: int, username: str, first_name: str):
    """Добавить админа бота"""
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO bot_admins (user_id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO NOTHING
        ''', user_id, username, first_name)

async def remove_bot_admin(user_id: int):
    """Удалить админа бота"""
    async with db_pool.acquire() as conn:
        await conn.execute('DELETE FROM bot_admins WHERE user_id = $1', user_id)

# Клавиатуры
def get_main_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Добавление и настройка объектов", callback_data="objects_menu")],
        [InlineKeyboardButton(text="🏢 Личный кабинет компании", callback_data="company_cabinet")],
        [InlineKeyboardButton(text="💡 Что улучшить в боте", callback_data="suggest_improvement")]
    ])

def get_add_company_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Добавить компанию", callback_data="add_company")]
    ])

def get_back_keyboard(callback="back"):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=callback)]
    ])

def get_company_cabinet_keyboard(company_info):
    long_term_text = "Да" if company_info['long_term_only'] else "Нет"
    
    # Преобразуем минуты в часовой пояс (формат UTC+X)
    timezone_minutes = company_info.get('timezone_offset', 0)
    timezone_hours = timezone_minutes / 60
    
    if timezone_hours >= 0:
        timezone_text = f"UTC+{int(timezone_hours)}"
    else:
        timezone_text = f"UTC{int(timezone_hours)}"
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Изменить название", callback_data="edit_company_name")],
        [InlineKeyboardButton(text="Изменить город", callback_data="edit_company_city")],
        [InlineKeyboardButton(text="Изменить приветствие", callback_data="edit_company_welcome")],
        [InlineKeyboardButton(text=f"Изменить часовой пояс: {timezone_text}", callback_data="edit_company_timezone")],
        [InlineKeyboardButton(text=f"Время заезда {company_info['checkin_time']}", callback_data="edit_checkin_time")],
        [InlineKeyboardButton(text=f"Только долгосрок: {long_term_text}", callback_data="toggle_long_term")],
        [InlineKeyboardButton(text=f"Время выезда {company_info['checkout_time']}", callback_data="edit_checkout_time")],
        [InlineKeyboardButton(text="Пригласить менеджера", callback_data="invite_manager")],
        [InlineKeyboardButton(text="Менеджеры", callback_data="managers_list")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])

def get_objects_list_keyboard(properties):
    buttons = []
    for prop_id, name, address, is_short_term in properties:
        buttons.append([InlineKeyboardButton(text=name, callback_data=f"property_{prop_id}")])
    
    buttons.append([InlineKeyboardButton(text="➕ Добавить объект", callback_data="add_property")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_property_menu_keyboard(property_id: int, is_short_term: bool = True):
    # Определяем текст кнопки в зависимости от режима
    term_button_text = "📅 Краткосрок" if is_short_term else "📅 Долгосрок"
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧳 Заселение", callback_data=f"section_checkin_{property_id}")],
        [InlineKeyboardButton(text="📹 Аренда", callback_data=f"section_rent_{property_id}")],
        [InlineKeyboardButton(text="🍿 Впечатления", callback_data=f"section_experiences_{property_id}")],
        [InlineKeyboardButton(text="📦 Выселение", callback_data=f"section_checkout_{property_id}")],
        [InlineKeyboardButton(text="🔗 Бронирования", callback_data=f"bookings_{property_id}")],
        [InlineKeyboardButton(text=term_button_text, callback_data=f"toggle_shortterm_{property_id}")],
        [InlineKeyboardButton(text="Ссылка на объект для собственника", callback_data=f"owner_link_{property_id}")],
        [InlineKeyboardButton(text="Редактировать объект", callback_data=f"edit_property_{property_id}")],
        [InlineKeyboardButton(text="Предпросмотр объекта", callback_data=f"prop_preview_{property_id}")],
        [InlineKeyboardButton(text="Удалить объект", callback_data=f"delete_property_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="objects_menu")]
    ])

async def get_checkin_section_keyboard_async(property_id: int, filled_fields: set = None):
    """Асинхронная клавиатура раздела Заселение с кастомными кнопками"""
    filled_fields = filled_fields or set()
    
    def field_text(name: str, key: str) -> str:
        return f"{name} ■" if key in filled_fields else name
    
    buttons = [
        [InlineKeyboardButton(text=field_text("🕐 Время заселения и выселения", "checkin_time"), callback_data=f"field_checkin_time_{property_id}")],
        [InlineKeyboardButton(text=field_text("🚗 Парковка", "parking"), callback_data=f"field_parking_{property_id}")],
        [InlineKeyboardButton(text=field_text("🌐 Wi-Fi", "wifi"), callback_data=f"field_wifi_{property_id}")],
        [InlineKeyboardButton(text=field_text("🔑 Ключ от двери", "door_key"), callback_data=f"field_door_key_{property_id}")],
        [InlineKeyboardButton(text=field_text("🗺 Как найти объект?", "how_to_find"), callback_data=f"field_how_to_find_{property_id}")],
        [InlineKeyboardButton(text=field_text("🚶 Как дойти до квартиры", "how_to_reach"), callback_data=f"field_how_to_reach_{property_id}")],
        [InlineKeyboardButton(text=field_text("📄 Документы для заселения", "documents"), callback_data=f"field_documents_{property_id}")],
        [InlineKeyboardButton(text=field_text("💰 Депозит", "deposit"), callback_data=f"field_deposit_{property_id}")],
        [InlineKeyboardButton(text=field_text("🔐 Дистанционное заселение", "remote_checkin"), callback_data=f"field_remote_checkin_{property_id}")],
        [InlineKeyboardButton(text="🏠 Помощь с проживанием", callback_data=f"subsection_help_{property_id}")],
        [InlineKeyboardButton(text="📍 Магазины, аптеки итд.", callback_data=f"subsection_stores_{property_id}")],
        [InlineKeyboardButton(text=field_text("📢 Правила проживания", "rules"), callback_data=f"field_rules_{property_id}")],
    ]
    
    # Добавляем кастомные кнопки
    custom_fields = await get_custom_fields(property_id, 'checkin')
    for custom in custom_fields:
        custom_name = field_text(custom['field_name'], custom['field_key'])
        buttons.append([InlineKeyboardButton(text=custom_name, callback_data=f"custom_field_{property_id}_checkin_{custom['field_key']}")])
    
    buttons.append([InlineKeyboardButton(text="➕ Добавить кнопку", callback_data=f"add_custom_checkin_{property_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"property_{property_id}")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_rent_section_keyboard(property_id: int, filled_fields: set = None):
    """Клавиатура раздела Аренда с индикаторами заполненности"""
    filled_fields = filled_fields or set()
    
    def field_text(name: str, key: str) -> str:
        return f"{name} ■" if key in filled_fields else name
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=field_text("📱 Телефоны УК", "uk_phones"), callback_data=f"field_uk_phones_{property_id}")],
        [InlineKeyboardButton(text=field_text("👨‍💼 Телефон диспетчера", "dispatcher"), callback_data=f"field_dispatcher_{property_id}")],
        [InlineKeyboardButton(text=field_text("🆘 Телефон аварийной службы", "emergency"), callback_data=f"field_emergency_{property_id}")],
        [InlineKeyboardButton(text=field_text("💬 Домовые чаты", "chats"), callback_data=f"field_chats_{property_id}")],
        [InlineKeyboardButton(text=field_text("📝 Форма обратной связи", "feedback_form"), callback_data=f"field_feedback_form_{property_id}")],
        [InlineKeyboardButton(text=field_text("🌐 Интернет", "internet"), callback_data=f"field_internet_{property_id}")],
        [InlineKeyboardButton(text="➕ Добавить кнопку", callback_data=f"add_custom_rent_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"property_{property_id}")]
    ])

def get_help_subsection_keyboard(property_id: int, filled_fields: set = None):
    """Клавиатура подраздела Помощь с проживанием"""
    filled_fields = filled_fields or set()
    
    def field_text(name: str, key: str) -> str:
        return f"{name} ■" if key in filled_fields else name
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=field_text("🥐 Завтрак", "breakfast"), callback_data=f"field_breakfast_{property_id}")],
        [InlineKeyboardButton(text=field_text("🛏 Поменять бельё", "linen"), callback_data=f"field_linen_{property_id}")],
        [InlineKeyboardButton(text=field_text("📱 Связь с менеджером", "manager_contact"), callback_data=f"field_manager_contact_{property_id}")],
        [InlineKeyboardButton(text=field_text("📺 Настройка ТВ", "tv_setup"), callback_data=f"field_tv_setup_{property_id}")],
        [InlineKeyboardButton(text=field_text("❄️ Кондиционер", "ac"), callback_data=f"field_ac_{property_id}")],
        [InlineKeyboardButton(text="➕ Добавить кнопку", callback_data=f"add_custom_help_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_checkin_{property_id}")]
    ])

def get_stores_subsection_keyboard(property_id: int, filled_fields: set = None):
    """Клавиатура подраздела Магазины, аптеки"""
    filled_fields = filled_fields or set()
    
    def field_text(name: str, key: str) -> str:
        return f"{name} ■" if key in filled_fields else name
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=field_text("🛒 Магазины", "shops"), callback_data=f"field_shops_{property_id}")],
        [InlineKeyboardButton(text=field_text("🚗 Аренда машин", "car_rental"), callback_data=f"field_car_rental_{property_id}")],
        [InlineKeyboardButton(text=field_text("🏃 Спорт", "sport"), callback_data=f"field_sport_{property_id}")],
        [InlineKeyboardButton(text=field_text("💊 Больницы", "hospitals"), callback_data=f"field_hospitals_{property_id}")],
        [InlineKeyboardButton(text="➕ Добавить кнопку", callback_data=f"add_custom_stores_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_checkin_{property_id}")]
    ])

def get_experiences_section_keyboard(property_id: int, filled_fields: set = None):
    """Клавиатура раздела Впечатления"""
    filled_fields = filled_fields or set()
    
    def field_text(name: str, key: str) -> str:
        return f"{name} ■" if key in filled_fields else name
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=field_text("🗿 Экскурсии", "excursions"), callback_data=f"field_excursions_{property_id}")],
        [InlineKeyboardButton(text=field_text("🏛 Музеи", "museums"), callback_data=f"field_museums_{property_id}")],
        [InlineKeyboardButton(text=field_text("🌳 Парки", "parks"), callback_data=f"field_parks_{property_id}")],
        [InlineKeyboardButton(text=field_text("🎬 Кино и театры", "entertainment"), callback_data=f"field_entertainment_{property_id}")],
        [InlineKeyboardButton(text="➕ Добавить кнопку", callback_data=f"add_custom_exp_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"property_{property_id}")]
    ])

def get_checkout_section_keyboard(property_id: int, filled_fields: set = None):
    """Клавиатура раздела Выселение"""
    filled_fields = filled_fields or set()
    
    def field_text(name: str, key: str) -> str:
        return f"{name} ■" if key in filled_fields else name
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=field_text("🚪 Как выехать без менеджера?", "self_checkout"), callback_data=f"field_self_checkout_{property_id}")],
        [InlineKeyboardButton(text=field_text("💸 Возврат депозита", "deposit_return"), callback_data=f"field_deposit_return_{property_id}")],
        [InlineKeyboardButton(text=field_text("📅 Продлить проживание", "extend_stay"), callback_data=f"field_extend_stay_{property_id}")],
        [InlineKeyboardButton(text=field_text("🎁 Скидки", "discounts"), callback_data=f"field_discounts_{property_id}")],
        [InlineKeyboardButton(text="➕ Добавить кнопку", callback_data=f"add_custom_checkout_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"property_{property_id}")]
    ])

def get_field_edit_keyboard(property_id: int, section: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_{section}_{property_id}")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"skip_field_{section}_{property_id}")]
    ])

# Маппинг полей с иконками (как на скриншотах)
FIELD_NAMES = {
    'checkin_time': '🕐 Время заселения и выселения',
    'parking': '🚗 Парковка',
    'wifi': '📶 Wi-Fi',
    'door_key': '🔑 Ключ от двери',
    'how_to_find': '🗺️ Как найти объект?',
    'how_to_reach': '🏢 Как дойти до квартиры',
    'documents': '📄 Документы для заселения',
    'deposit': '💰 Депозит',
    'remote_checkin': '🔒 Дистанционное заселение',
    'rules': '📋 Правила проживания',
    'breakfast': '🥐 Завтрак',
    'linen': '🛏 Поменять бельё',
    'manager_contact': '📱 Связь с менеджером',
    'tv_setup': '📺 Настройка ТВ',
    'ac': '❄️ Кондиционер',
    'shops': '🛒 Магазины',
    'car_rental': '🚗 Аренда машин',
    'sport': '🏃 Спорт',
    'hospitals': '💊 Больницы',
    'uk_phones': '🏢 Телефоны УК',
    'dispatcher': '👤 Телефон диспетчера',
    'emergency': '🆘 Телефон аварийной службы',
    'chats': '💬 Домовые чаты',
    'feedback_form': '📝 Форма обратной связи',
    'internet': '🌐 Интернет',
    'excursions': '🚌 Экскурсии',
    'museums': '🏛️ Музеи',
    'parks': '🖼️ Парки',
    'entertainment': '🎭 Кино и театры',
    'self_checkout': '🚪 Как выехать без менеджера?',
    'deposit_return': '💸 Возврат депозита',
    'extend_stay': '📅 Продлить проживание',
    'discounts': '🎁 Скидки'
}

FIELD_DESCRIPTIONS = {
    'checkin_time': 'Укажите время заезда и выезда для гостя',
    'parking': 'Расскажите, есть ли у ваших апартаментов парковка и где она находится',
    'wifi': 'Информация о работе Wi-fi в апартаментах',
    'door_key': 'Расскажите, есть ли в апартаментах сейф и какой от него пароль',
    'how_to_find': 'Здесь вы можете рассказать, с какой стороны подъехать к вашему подъезду, где находится вход и есть ли код у домофона или просто можете отправить геолокацию.',
    'how_to_reach': 'Покажите процесс как добраться до квартиры',
    'documents': 'Здесь вы можете прикрепить необходимые документы',
    'deposit': 'Здесь вы можете добавить информацию и залоге и правилах, при которых он будет возвращён',
    'remote_checkin': 'Расскажите, как проходит дистанционное заселение, где находится сейф и как получить пароль',
    'rules': 'Здесь вы можете добавить информацию и залоге и правилах, при которых он будет возвращён',
    'breakfast': 'Расскажите, возможен ли заказ завтрака в апартаменты и укажите стоимость для этой услуги',
    'linen': 'Укажите, возможность замены белья в апартаментах и стоимость этой услуги',
    'manager_contact': 'Информация о действия гостя в случае ЧП. Здесь вы можете оставить контактные данные или инструкции на такой случай',
    'tv_setup': 'Здесь можно упомянуть возможности и особенности вашего телевизора',
    'ac': 'Например: где находится пульт, что делать если кондиционер не работает',
    'shops': 'Расскажите, где поблизости находятся магазины',
    'car_rental': 'Расскажите, где поблизости можно взять в аренду автомобиль',
    'sport': 'Расскажите, где поблизости можно заняться спортом. Например, в парке или в спортзале',
    'hospitals': 'Расскажите, где поблизости находится больница или травмпункт',
    'internet': 'В этом разделе, вы можете добавить информацию о интернет провайдере. Также не забудьте написать призыв отправлять фото с чеком об оплате интернета',
    'excursions': 'Расскажите, какие в вашем городе или районе доступны экскурсии. Что интересного можно узнать о месте, где проживает гость.',
    'museums': 'Расскажите, какие музеи есть рядом и какое у них направление.',
    'parks': 'Расскажите, где можно погулять рядом с вашими апартаментами',
    'entertainment': 'Расскажите, какие у вас есть кинотеатры и театры поблизости. Также можно явно упомянуть, ближайшие события',
    'self_checkout': 'Расскажите, как можно выехать без участия менеджера. Какие шаги для этого необходимо выполнить?',
    'deposit_return': 'Укажите инструкции как продлить проживание. Также можно явно упомянуть, что гость может связаться из этой категории с менеджером отправив сообщение',
    'extend_stay': 'Укажите инструкции как продлить проживание. Также можно явно упомянуть, что гость может связаться из этой категории с менеджером отправив сообщение',
    'discounts': 'Здесь можно добавить различные скидки и акции для постоянных клиентов'
}

SECTION_ICONS = {
    'checkin': '🧳',
    'rent': '📹',
    'experiences': '🍿',
    'checkout': '📦'
}

SECTION_NAMES = {
    'checkin': 'Заселение',
    'rent': 'Аренда',
    'experiences': 'Впечатления',
    'checkout': 'Выселение'
}

# ============================================
# КОМАНДЫ ДЛЯ БЫСТРОГО ДОСТУПА
# ============================================

@dp.message(Command("menu"))
async def cmd_menu(message: types.Message, state: FSMContext):
    """Команда /menu - главное меню"""
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    # Автоматически устанавливаем company_id если его нет
    if not company_id:
        companies = await get_user_companies(message.from_user.id)
        if companies:
            company_id = companies[0][0]
            await state.update_data(current_company_id=company_id)
    
    await message.answer(
        "Главное меню.\n\nЗдесь вы можете настроить свою компанию и добавить объекты недвижимости.",
        reply_markup=get_main_menu_keyboard()
    )

@dp.message(Command("company"))
async def cmd_company(message: types.Message, state: FSMContext):
    """Команда /company - личный кабинет компании"""
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    # Автоматически устанавливаем company_id если его нет
    if not company_id:
        companies = await get_user_companies(message.from_user.id)
        if companies:
            company_id = companies[0][0]
            await state.update_data(current_company_id=company_id)
        else:
            await message.answer(
                "❌ Сначала создайте компанию.\n\nИспользуйте /start для начала работы.",
                reply_markup=get_add_company_keyboard()
            )
            return
    
    company_info = await get_company_info(company_id)
    
    if company_info:
        text = (
            f"{company_info['name']}\n"
            f"{company_info['city']}\n\n"
            f"Приветствие гостя:\n"
            f"{company_info['welcome_message']}\n\n"
            f"* в данном разделе вы можете менять настройки вашей компании"
        )
        await message.answer(text, reply_markup=get_company_cabinet_keyboard(company_info))
    else:
        await message.answer(
            "❌ Компания не найдена.\n\nИспользуйте /start для начала работы.",
            reply_markup=get_add_company_keyboard()
        )

@dp.message(Command("apartments"))
async def cmd_apartments(message: types.Message, state: FSMContext):
    """Команда /apartments - список объектов и их редактирование"""
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    # Автоматически устанавливаем company_id если его нет
    if not company_id:
        companies = await get_user_companies(message.from_user.id)
        if companies:
            company_id = companies[0][0]
            await state.update_data(current_company_id=company_id)
        else:
            await message.answer(
                "❌ Сначала создайте компанию.\n\nИспользуйте /start для начала работы.",
                reply_markup=get_add_company_keyboard()
            )
            return
    
    properties = await get_company_properties(company_id)
    
    if properties:
        count_text = f"📊 Всего объектов: {len(properties)}"
    else:
        count_text = "📭 У вас пока нет объектов"
    
    await message.answer(
        f"Вот список ваших объектов. Здесь вы можете добавлять и редактировать их.\n\n{count_text}",
        reply_markup=get_objects_list_keyboard(properties)
    )

# ============================================
# КОМАНДА /START
# ============================================

# Обработчик команды /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    # Сохраняем пользователя
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO users (user_id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO NOTHING
        ''', user_id, message.from_user.username, message.from_user.first_name)
    
    # Проверяем параметры старта
    start_param = message.text.split()[1] if len(message.text.split()) > 1 else None
    
    # Режим гостя
    if start_param and start_param.startswith("guest_"):
        access_code = start_param.replace("guest_", "")
        booking = await get_booking_by_code(access_code)
        
        if booking and booking['is_active']:
            property_id = booking['property_id']
            property_name = booking['property_name']
            address = booking['address'] or "МОСква"
            
            text = f"{property_name}\n\nАдрес апартаментов: {address}.\n\nВот информация, доступная для изучения:"
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➡️ Начать", callback_data=f"guest_start_{property_id}")],
                [InlineKeyboardButton(text="🚕 Вызвать такси", url="https://taxi.yandex.ru")]
            ])
            
            await message.answer(text, reply_markup=keyboard)
            return
        else:
            await message.answer("Бронирование не найдено или неактивно. Обратитесь к менеджеру.")
            return
    
    # Присоединение к компании по инвайт-коду
    if start_param and start_param.startswith("org_"):
        invite_code = start_param.replace("org_", "")
        company_id = await join_company_by_invite(user_id, invite_code)
        
        if company_id:
            await state.update_data(current_company_id=company_id)
            company_info = await get_company_info(company_id)
            await message.answer(f"✅ Вы успешно присоединились к компании «{company_info['name']}»!")
            
            text = (
                "Вы в главном меню бота 🏠\n\n"
                "Если вы хотите добавить апартаменты и поделиться ссылкой с гостями, "
                "переходите в раздел «Добавление и настройка объектов»"
            )
            await message.answer(text, reply_markup=get_main_menu_keyboard())
            return
        else:
            await message.answer("Неверная ссылка приглашения или компания не найдена.")
            return
    
    # Режим менеджера
    companies = await get_user_companies(user_id)
    first_start = await is_first_start(user_id)
    
    if not companies:
        # Показываем приветственное сообщение только при первом запуске
        if first_start:
            await mark_user_not_first_start(user_id)
            text = (
                "Для того, чтобы пользоваться ботом, вам необходимо выбрать компанию. "
                "Если ваши коллеги уже создали компанию, необходимо, чтобы они поделились с вами пригласительной ссылкой.\n\n"
                "Если вы хотите создать свою компанию, нажмите на кнопку «Добавить компанию».\n\n"
                "К этому сообщению мы прикрепили подробную инструкцию как пользоваться ботом. "
                "Вы сможете вернуться к ней позже, если потребуется."
            )
        else:
            text = "Создайте компанию или присоединитесь к существующей по ссылке-приглашению."
        
        await message.answer(text, reply_markup=get_add_company_keyboard())
    else:
        await state.update_data(current_company_id=companies[0][0])
        text = (
            "Вы в главном меню бота 🏠\n\n"
            "Если вы хотите добавить апартаменты и поделиться ссылкой с гостями, "
            "переходите в раздел «Добавление и настройка объектов»\n\n"
            "Если вы хотите изменить общие настройки компании или её название и город, "
            "переходите в раздел «Личный кабинет компании»"
        )
        await message.answer(text, reply_markup=get_main_menu_keyboard())

# Создание компании
@dp.callback_query(F.data == "add_company")
async def add_company(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Напишите название компании и нажмите ввод 👇",
        reply_markup=get_back_keyboard("start")
    )
    await state.set_state(CompanyStates.waiting_company_name)
    await callback.answer()

# Обработчик отмены создания компании
@dp.callback_query(F.data == "cancel")
async def cancel_company_creation(callback: types.CallbackQuery, state: FSMContext):
    """Отмена создания компании"""
    await state.clear()
    
    companies = await get_user_companies(callback.from_user.id)
    
    if companies:
        # Если есть компании - возвращаем в главное меню
        await state.update_data(current_company_id=companies[0][0])
        text = (
            "Вы в главном меню бота 🏠\n\n"
            "Если вы хотите добавить апартаменты и поделиться ссылкой с гостями, "
            "переходите в раздел «Добавление и настройка объектов»"
        )
        await callback.message.edit_text(text, reply_markup=get_main_menu_keyboard())
    else:
        # Если нет компаний - предлагаем создать
        text = "Создание компании отменено.\n\nСоздайте компанию или присоединитесь к существующей по ссылке-приглашению."
        await callback.message.edit_text(text, reply_markup=get_add_company_keyboard())
    
    await callback.answer("Отменено")

@dp.message(CompanyStates.waiting_company_name)
async def process_company_name(message: types.Message, state: FSMContext):
    await state.update_data(company_name=message.text)
    await message.answer(
        "Напишите город компании и нажмите ввод 👇",
        reply_markup=get_back_keyboard("cancel")
    )
    await state.set_state(CompanyStates.waiting_company_city)

@dp.message(CompanyStates.waiting_company_city)
async def process_company_city(message: types.Message, state: FSMContext):
    data = await state.get_data()
    
    # ИСПРАВЛЕНИЕ: Защита от потери state между шагами
    company_name = data.get('company_name')
    
    if not company_name:
        # State был очищен или потерян - начинаем заново
        await state.clear()
        await message.answer(
            "❌ Произошла ошибка. Давайте начнём сначала.\n\nНапишите название компании:",
            reply_markup=get_back_keyboard("start")
        )
        await state.set_state(CompanyStates.waiting_company_name)
        logger.warning(f"Lost company_name in state for user {message.from_user.id}")
        return
    
    company_city = message.text
    
    company_id = await create_company(company_name, company_city, message.from_user.id)
    
    # ИСПРАВЛЕНО: Сохраняем company_id в state ПЕРЕД clear()
    await state.update_data(current_company_id=company_id)
    
    await message.answer(
        f"Отлично! Компания создана.\n\nНазвание: {company_name}\nГород: {company_city}",
        reply_markup=get_main_menu_keyboard()
    )
    
    # Очищаем только временные данные, оставляем current_company_id
    await state.set_data({'current_company_id': company_id})

# Главное меню
@dp.callback_query(F.data == "main_menu")
async def main_menu(callback: types.CallbackQuery):
    text = (
        "Вы в главном меню бота 🏠\n\n"
        "Если вы хотите добавить апартаменты и поделиться ссылкой с гостями, "
        "переходите в раздел «Добавление и настройка объектов»"
    )
    await callback.message.edit_text(text, reply_markup=get_main_menu_keyboard())
    await callback.answer()

# Личный кабинет компании
@dp.callback_query(F.data == "company_cabinet")
async def company_cabinet(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    # ИСПРАВЛЕНО: Если компания не выбрана, пытаемся получить первую компанию пользователя
    if not company_id:
        companies = await get_user_companies(callback.from_user.id)
        if companies:
            company_id = companies[0][0]
            await state.update_data(current_company_id=company_id)
        else:
            await callback.message.edit_text(
                "Сначала создайте компанию",
                reply_markup=get_add_company_keyboard()
            )
            await callback.answer("⚠️ Сначала создайте компанию", show_alert=True)
            return
    
    company_info = await get_company_info(company_id)
    
    if company_info:
        text = (
            f"{company_info['name']}\n"
            f"{company_info['city']}\n\n"
            f"Приветствие гостя:\n"
            f"{company_info['welcome_message']}\n\n"
            f"* в данном разделе вы можете менять настройки вашей компании"
        )
        await callback.message.edit_text(text, reply_markup=get_company_cabinet_keyboard(company_info))
    
    await callback.answer()

# Редактирование компании
@dp.callback_query(F.data == "edit_company_name")
async def edit_company_name(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Напишите название компании и нажмите ввод 👇",
        reply_markup=get_back_keyboard("company_cabinet")
    )
    await state.set_state(CompanyStates.editing_company_name)
    await callback.answer()

@dp.message(CompanyStates.editing_company_name)
async def process_edit_company_name(message: types.Message, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    await update_company_field(company_id, 'name', message.text)
    
    company_info = await get_company_info(company_id)
    text = (
        f"{company_info['name']}\n"
        f"{company_info['city']}\n\n"
        f"Приветствие гостя:\n"
        f"{company_info['welcome_message']}\n\n"
        f"* в данном разделе вы можете менять настройки вашей компании"
    )
    await message.answer(text, reply_markup=get_company_cabinet_keyboard(company_info))
    
    # ИСПРАВЛЕНО: Сохраняем company_id в state
    await state.set_data({'current_company_id': company_id})

@dp.callback_query(F.data == "edit_company_city")
async def edit_company_city(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Напишите город компании и нажмите ввод 👇",
        reply_markup=get_back_keyboard("company_cabinet")
    )
    await state.set_state(CompanyStates.editing_company_city)
    await callback.answer()

@dp.message(CompanyStates.editing_company_city)
async def process_edit_company_city(message: types.Message, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    await update_company_field(company_id, 'city', message.text)
    
    company_info = await get_company_info(company_id)
    text = (
        f"{company_info['name']}\n"
        f"{company_info['city']}\n\n"
        f"Приветствие гостя:\n"
        f"{company_info['welcome_message']}\n\n"
        f"* в данном разделе вы можете менять настройки вашей компании"
    )
    await message.answer(text, reply_markup=get_company_cabinet_keyboard(company_info))
    await clear_state_keep_company(state)

@dp.callback_query(F.data == "edit_company_welcome")
async def edit_company_welcome(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Вы редактируете кнопку\n\nВведите приветствие кнопки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="company_cabinet")],
            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="company_cabinet")]
        ])
    )
    await state.set_state(CompanyStates.editing_company_welcome)
    await callback.answer()

@dp.message(CompanyStates.editing_company_welcome)
async def process_edit_company_welcome(message: types.Message, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    await update_company_field(company_id, 'welcome_message', message.text)
    
    company_info = await get_company_info(company_id)
    text = (
        f"{company_info['name']}\n"
        f"{company_info['city']}\n\n"
        f"Приветствие гостя:\n"
        f"{company_info['welcome_message']}\n\n"
        f"* в данном разделе вы можете менять настройки вашей компании"
    )
    await message.answer(text, reply_markup=get_company_cabinet_keyboard(company_info))
    await clear_state_keep_company(state)

@dp.callback_query(F.data == "edit_company_timezone")
async def edit_company_timezone(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Вы редактируете кнопку\n\nВведите часовой пояс вашей компании.\n\nПримеры:\n+3 для Москвы (UTC+3)\n+5 для Екатеринбурга (UTC+5)\n+7 для Новосибирска (UTC+7)\n\nВведите число:",
        reply_markup=get_back_keyboard("company_cabinet")
    )
    await state.set_state(CompanyStates.waiting_timezone)
    await callback.answer()

@dp.message(CompanyStates.waiting_timezone)
async def process_edit_timezone(message: types.Message, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    try:
        # Пользователь вводит часы (например, +3), мы храним минуты
        timezone_hours = int(message.text.replace('+', ''))
        timezone_minutes = timezone_hours * 60
        
        await update_company_field(company_id, 'timezone_offset', timezone_minutes)
        
        company_info = await get_company_info(company_id)
        text = (
            f"{company_info['name']}\n"
            f"{company_info['city']}\n\n"
            f"Приветствие гостя:\n"
            f"{company_info['welcome_message']}\n\n"
            f"* в данном разделе вы можете менять настройки вашей компании"
        )
        await message.answer(text, reply_markup=get_company_cabinet_keyboard(company_info))
        await clear_state_keep_company(state)
    except ValueError:
        await message.answer("Пожалуйста, введите число (например, +3 для Москвы)")

@dp.callback_query(F.data == "edit_checkin_time")
async def edit_checkin_time(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Вы редактируете кнопку\n\nВведите время заезда в формате 12:00:",
        reply_markup=get_back_keyboard("company_cabinet")
    )
    await state.set_state(CompanyStates.waiting_checkin_time)
    await callback.answer()

@dp.message(CompanyStates.waiting_checkin_time)
async def process_edit_checkin_time(message: types.Message, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    await update_company_field(company_id, 'checkin_time', message.text)
    
    company_info = await get_company_info(company_id)
    text = (
        f"{company_info['name']}\n"
        f"{company_info['city']}\n\n"
        f"Приветствие гостя:\n"
        f"{company_info['welcome_message']}\n\n"
        f"* в данном разделе вы можете менять настройки вашей компании"
    )
    await message.answer(text, reply_markup=get_company_cabinet_keyboard(company_info))
    await clear_state_keep_company(state)

@dp.callback_query(F.data == "edit_checkout_time")
async def edit_checkout_time(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Вы редактируете кнопку\n\nВведите время выезда в формате 12:00:",
        reply_markup=get_back_keyboard("company_cabinet")
    )
    await state.set_state(CompanyStates.waiting_checkout_time)
    await callback.answer()

@dp.message(CompanyStates.waiting_checkout_time)
async def process_edit_checkout_time(message: types.Message, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    await update_company_field(company_id, 'checkout_time', message.text)
    
    company_info = await get_company_info(company_id)
    text = (
        f"{company_info['name']}\n"
        f"{company_info['city']}\n\n"
        f"Приветствие гостя:\n"
        f"{company_info['welcome_message']}\n\n"
        f"* в данном разделе вы можете менять настройки вашей компании"
    )
    await message.answer(text, reply_markup=get_company_cabinet_keyboard(company_info))
    await clear_state_keep_company(state)

@dp.callback_query(F.data == "toggle_long_term")
async def toggle_long_term(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    async with db_pool.acquire() as conn:
        await conn.execute('UPDATE companies SET long_term_only = NOT long_term_only WHERE id = $1', company_id)
    
    company_info = await get_company_info(company_id)
    text = (
        f"{company_info['name']}\n"
        f"{company_info['city']}\n\n"
        f"Приветствие гостя:\n"
        f"{company_info['welcome_message']}\n\n"
        f"* в данном разделе вы можете менять настройки вашей компании"
    )
    await callback.message.edit_text(text, reply_markup=get_company_cabinet_keyboard(company_info))
    await callback.answer()

@dp.callback_query(F.data == "invite_manager")
async def invite_manager(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    company_info = await get_company_info(company_id)
    bot_username = (await bot.get_me()).username
    invite_link = f"https://t.me/{bot_username}?start=org_{company_info['invite_code']}"
    
    text = (
        f"Ссылка для приглашения менеджера в компанию «{company_info['name']}»:\n\n"
        f"{invite_link}\n\n"
        f"По-умолчанию менеджер не может удалять объекты."
    )
    
    await callback.message.answer(text)
    await callback.answer()

@dp.callback_query(F.data == "managers_list")
async def managers_list(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    if not company_id:
        await callback.answer("Ошибка: компания не выбрана", show_alert=True)
        return
    
    managers = await get_company_managers(company_id)
    
    text = (
        "Вы на странице менеджеров. Ниже вы можете видеть сотрудников вашей компании. "
        "Здесь вы можете назначить менеджера администратором и дать ему возможность удалять объекты.\n\n"
    )
    
    buttons = []
    
    if managers and len(managers) > 0:
        text += "📋 Список менеджеров:\n\n"
        for manager in managers:
            username = manager['username'] or "Без username"
            first_name = manager['first_name'] or "Без имени"
            is_admin = manager['is_admin']
            
            role = "👑 Админ" if is_admin else "👤 Менеджер"
            manager_text = f"{role} - {first_name} (@{username})"
            
            text += f"• {manager_text}\n"
            
            # Кнопка для каждого менеджера
            button_text = f"{first_name} - {role}"
            buttons.append([InlineKeyboardButton(
                text=button_text,
                callback_data=f"manage_user_{manager['user_id']}"
            )])
    else:
        text += "У вас нет приглашённых менеджеров"
    
    buttons.append([InlineKeyboardButton(text="Пригласить менеджера", callback_data="invite_manager")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="company_cabinet")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# Меню объектов
@dp.callback_query(F.data == "objects_menu")
async def objects_menu(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    # ИСПРАВЛЕНО: Если компания не выбрана, пытаемся получить первую компанию пользователя
    if not company_id:
        companies = await get_user_companies(callback.from_user.id)
        if companies:
            company_id = companies[0][0]
            await state.update_data(current_company_id=company_id)
        else:
            await callback.message.edit_text(
                "Сначала создайте компанию",
                reply_markup=get_add_company_keyboard()
            )
            await callback.answer("⚠️ Сначала создайте компанию", show_alert=True)
            return
    
    properties = await get_company_properties(company_id)
    await callback.message.edit_text(
        "Вот список ваших объектов. Здесь вы можете добавлять и редактировать их.",
        reply_markup=get_objects_list_keyboard(properties)
    )
    await callback.answer()

# Добавление объекта
@dp.callback_query(F.data == "add_property")
async def add_property(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Вы редактируете кнопку\n\nВведите название объекта:",
        reply_markup=get_back_keyboard("objects_menu")
    )
    await state.set_state(PropertyStates.waiting_property_name)
    await callback.answer()

@dp.message(PropertyStates.waiting_property_name)
async def process_property_name(message: types.Message, state: FSMContext):
    await state.update_data(property_name=message.text)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="objects_menu")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip_address")]
    ])
    await message.answer("Введите адрес объекта:", reply_markup=keyboard)
    await state.set_state(PropertyStates.waiting_property_address)

@dp.message(PropertyStates.waiting_property_address)
async def process_property_address(message: types.Message, state: FSMContext):
    data = await state.get_data()
    property_name = data.get('property_name')
    company_id = data.get('current_company_id')
    
    # ИСПРАВЛЕНИЕ: Защита от потери state
    if not property_name:
        await state.clear()
        await state.update_data(current_company_id=company_id)  # Восстанавливаем company_id
        await message.answer(
            "❌ Произошла ошибка. Давайте начнём сначала.\n\nВведите название объекта:",
            reply_markup=get_back_keyboard("objects_menu")
        )
        await state.set_state(PropertyStates.waiting_property_name)
        logger.warning(f"Lost property_name in state for user {message.from_user.id}")
        return
    
    property_address = message.text
    
    property_id = await create_property(company_id, property_name, property_address)
    
    # ИСПРАВЛЕНО: Сохраняем property_id но НЕ очищаем state (нужен company_id для confirm_save)
    await state.update_data(pending_property_id=property_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 Сохранить", callback_data=f"confirm_save_{property_id}")],
        [InlineKeyboardButton(text="❌ Не сохранять", callback_data="objects_menu")]
    ])
    
    await message.answer("Сохранить объект?", reply_markup=keyboard)
    # НЕ очищаем state здесь!

@dp.callback_query(F.data.startswith("confirm_save_"))
async def confirm_save(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    # ИСПРАВЛЕНО: Сначала получаем список с правильным company_id
    properties = await get_company_properties(company_id)
    
    # Теперь можем очистить state, сохранив только company_id
    await clear_state_keep_company(state)
    
    await callback.message.edit_text(
        "Вот список ваших объектов. Здесь вы можете добавлять и редактировать их.",
        reply_markup=get_objects_list_keyboard(properties)
    )
    await callback.answer("Объект сохранен!")

@dp.callback_query(F.data == "skip_address")
async def skip_address(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    property_name = data.get('property_name')
    company_id = data.get('current_company_id')
    
    # ИСПРАВЛЕНИЕ: Защита от потери state
    if not property_name:
        await state.clear()
        await state.update_data(current_company_id=company_id)  # Восстанавливаем company_id
        await callback.message.edit_text(
            "❌ Произошла ошибка. Давайте начнём сначала.\n\nВведите название объекта:",
            reply_markup=get_back_keyboard("objects_menu")
        )
        await state.set_state(PropertyStates.waiting_property_name)
        await callback.answer("⚠️ Данные потеряны, начните заново")
        logger.warning(f"Lost property_name in state for user {callback.from_user.id}")
        return
    
    property_id = await create_property(company_id, property_name, "")
    
    # ИСПРАВЛЕНО: Сохраняем property_id но НЕ очищаем state
    await state.update_data(pending_property_id=property_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 Сохранить", callback_data=f"confirm_save_{property_id}")],
        [InlineKeyboardButton(text="❌ Не сохранять", callback_data="objects_menu")]
    ])
    
    await callback.message.edit_text("Сохранить объект?", reply_markup=keyboard)
    # НЕ очищаем state здесь!
    await callback.answer()

# Просмотр объекта
@dp.callback_query(F.data.startswith("property_") & ~F.data.startswith("prop_preview_"))
async def view_property(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[1])
    property_info = await get_property_info(property_id)
    
    if property_info:
        property_name = property_info['name']
        is_short_term = property_info['is_short_term']
        
        text = f"Вы на странице объекта {property_name}.\n\nТут вы можете отредактировать информацию о объекте, которая будет доступна гостям."
        await callback.message.edit_text(text, reply_markup=get_property_menu_keyboard(property_id, is_short_term))
    
    await callback.answer()

# Редактирование объекта - ФУНКЦИОНАЛЬНАЯ РЕАЛИЗАЦИЯ
@dp.callback_query(F.data.startswith("edit_property_"))
async def edit_property_info(callback: types.CallbackQuery, state: FSMContext):
    property_id = int(callback.data.split("_")[2])
    
    # Получаем текущую информацию об объекте
    property_info = await get_property_info(property_id)
    
    if not property_info:
        await callback.answer("Объект не найден", show_alert=True)
        return
    
    property_name = property_info['name']
    property_address = property_info['address'] or "Не указан"
    
    # Показываем меню редактирования
    text = f"Редактирование объекта\n\n📝 Название: {property_name}\n📍 Адрес: {property_address}\n\nВыберите, что хотите изменить:"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Изменить название", callback_data=f"edit_prop_name_{property_id}")],
        [InlineKeyboardButton(text="Изменить адрес", callback_data=f"edit_prop_addr_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"property_{property_id}")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# Редактирование названия объекта - шаг 1: запрос названия
@dp.callback_query(F.data.startswith("edit_prop_name_"))
async def edit_property_name_start(callback: types.CallbackQuery, state: FSMContext):
    property_id = int(callback.data.split("_")[3])
    
    await state.update_data(editing_property_id=property_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"edit_property_{property_id}")],
        [InlineKeyboardButton(text="Пропустить", callback_data=f"edit_property_{property_id}")]
    ])
    
    await callback.message.edit_text(
        "Вы редактируете кнопку\n\nВведите название объекта:",
        reply_markup=keyboard
    )
    await state.set_state(PropertyStates.editing_property_name)
    await callback.answer()

# Редактирование названия объекта - шаг 2: сохранение названия
@dp.message(PropertyStates.editing_property_name)
async def process_edit_property_name(message: types.Message, state: FSMContext):
    data = await state.get_data()
    property_id = data['editing_property_id']
    new_name = message.text
    
    # Сохраняем новое название
    await state.update_data(new_property_name=new_name)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"edit_property_{property_id}")],
        [InlineKeyboardButton(text="Сохранить", callback_data=f"confirm_prop_edit_{property_id}")],
        [InlineKeyboardButton(text="Не сохранять", callback_data=f"edit_property_{property_id}")]
    ])
    
    await message.answer("Сохранить объект?", reply_markup=keyboard)

# Редактирование адреса объекта - шаг 1: запрос адреса
@dp.callback_query(F.data.startswith("edit_prop_addr_"))
async def edit_property_address_start(callback: types.CallbackQuery, state: FSMContext):
    property_id = int(callback.data.split("_")[3])
    
    await state.update_data(editing_property_id=property_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"edit_property_{property_id}")],
        [InlineKeyboardButton(text="Пропустить", callback_data=f"edit_property_{property_id}")]
    ])
    
    await callback.message.edit_text(
        "Введите адрес объекта:",
        reply_markup=keyboard
    )
    await state.set_state(PropertyStates.editing_property_address)
    await callback.answer()

# Редактирование адреса объекта - шаг 2: сохранение адреса
@dp.message(PropertyStates.editing_property_address)
async def process_edit_property_address(message: types.Message, state: FSMContext):
    data = await state.get_data()
    property_id = data['editing_property_id']
    new_address = message.text
    
    # Сохраняем новый адрес
    await state.update_data(new_property_address=new_address)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"edit_property_{property_id}")],
        [InlineKeyboardButton(text="Сохранить", callback_data=f"confirm_prop_edit_{property_id}")],
        [InlineKeyboardButton(text="Не сохранять", callback_data=f"edit_property_{property_id}")]
    ])
    
    await message.answer("Сохранить объект?", reply_markup=keyboard)

# Подтверждение сохранения изменений объекта
@dp.callback_query(F.data.startswith("confirm_prop_edit_"))
async def confirm_property_edit(callback: types.CallbackQuery, state: FSMContext):
    property_id = int(callback.data.split("_")[3])
    data = await state.get_data()
    
    # Обновляем название если было изменено
    new_name = data.get('new_property_name')
    if new_name:
        async with db_pool.acquire() as conn:
            await conn.execute('UPDATE properties SET name = $1 WHERE id = $2', new_name, property_id)
    
    # Обновляем адрес если был изменен
    new_address = data.get('new_property_address')
    if new_address:
        async with db_pool.acquire() as conn:
            await conn.execute('UPDATE properties SET address = $1 WHERE id = $2', new_address, property_id)
    
    # Очищаем временные данные но сохраняем company_id
    await clear_state_keep_company(state)
    
    # Получаем обновленную информацию
    property_info = await get_property_info(property_id)
    property_name = property_info['name']
    is_short_term = property_info['is_short_term']
    
    text = f"Вы на странице объекта {property_name}.\n\nТут вы можете отредактировать информацию о объекте, которая будет доступна гостям."
    await callback.message.edit_text(text, reply_markup=get_property_menu_keyboard(property_id, is_short_term))
    await callback.answer("✅ Изменения сохранены!")

# Разделы объекта
@dp.callback_query(F.data.startswith("section_checkin_"))
async def section_checkin(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    
    # Получаем заполненные поля включая help и stores подразделы
    filled_checkin = await get_filled_fields(property_id, 'checkin')
    filled_help = await get_filled_fields(property_id, 'help')
    filled_stores = await get_filled_fields(property_id, 'stores')
    
    # Объединяем все заполненные поля
    all_filled = filled_checkin | filled_help | filled_stores
    
    keyboard = await get_checkin_section_keyboard_async(property_id, all_filled)
    
    await callback.message.edit_text(
        "Вы на странице категории 🧳 Заселение",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("section_rent_"))
async def section_rent(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    filled_fields = await get_filled_fields(property_id, 'rent')
    
    await callback.message.edit_text(
        "Вы на странице категории 📹 Аренда",
        reply_markup=get_rent_section_keyboard(property_id, filled_fields)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("subsection_help_"))
async def subsection_help(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    filled_fields = await get_filled_fields(property_id, 'help')
    
    await callback.message.edit_text(
        "Вы на странице категории 🏠 Помощь с проживанием",
        reply_markup=get_help_subsection_keyboard(property_id, filled_fields)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("subsection_stores_"))
async def subsection_stores(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    filled_fields = await get_filled_fields(property_id, 'stores')
    
    await callback.message.edit_text(
        "Вы на странице категории 📍 Магазины, аптеки итд.",
        reply_markup=get_stores_subsection_keyboard(property_id, filled_fields)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("section_experiences_"))
async def section_experiences(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    filled_fields = await get_filled_fields(property_id, 'experiences')
    
    await callback.message.edit_text(
        "Вы на странице категории 🍿 Впечатления",
        reply_markup=get_experiences_section_keyboard(property_id, filled_fields)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("section_checkout_"))
async def section_checkout(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    filled_fields = await get_filled_fields(property_id, 'checkout')
    
    await callback.message.edit_text(
        "Вы на странице категории 📦 Выселение",
        reply_markup=get_checkout_section_keyboard(property_id, filled_fields)
    )
    await callback.answer()

# Редактирование полей
@dp.callback_query(F.data.startswith("field_"))
async def edit_field(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    field_key = "_".join(parts[1:-1])
    property_id = int(parts[-1])
    
    field_name = FIELD_NAMES.get(field_key, "Поле")
    field_desc = FIELD_DESCRIPTIONS.get(field_key, "Введите содержимое кнопки:")
    
    # Определяем секцию
    section = "checkin"
    if field_key in ['breakfast', 'linen', 'manager_contact', 'tv_setup', 'ac']:
        section = "help"
    elif field_key in ['shops', 'car_rental', 'sport', 'hospitals']:
        section = "stores"
    elif field_key in ['uk_phones', 'dispatcher', 'emergency', 'chats', 'feedback_form', 'internet']:
        section = "rent"
    elif field_key in ['excursions', 'museums', 'parks', 'entertainment']:
        section = "experiences"
    elif field_key in ['self_checkout', 'deposit_return', 'extend_stay', 'discounts']:
        section = "checkout"
    
    await state.update_data(
        editing_property_id=property_id,
        editing_field_key=field_key,
        editing_field_name=field_name,
        editing_section=section
    )
    
    text = f"Вы редактируете кнопку\n\n{field_desc}\n\nМожно добавить текст, фото, видео или документ.\n\nВведите содержимое кнопки:"
    
    await callback.message.edit_text(text, reply_markup=get_field_edit_keyboard(property_id, section))
    await state.set_state(PropertyStates.editing_field)
    await callback.answer()

@dp.message(PropertyStates.editing_field)
async def process_field_content(message: types.Message, state: FSMContext):
    data = await state.get_data()
    property_id = data['editing_property_id']
    field_key = data['editing_field_key']
    field_name = data['editing_field_name']
    section = data['editing_section']
    
    text_content = None
    file_id = None
    file_type = None
    
    if message.text:
        text_content = message.text
    elif message.photo:
        file_id = message.photo[-1].file_id
        file_type = "photo"
        text_content = message.caption
    elif message.video:
        file_id = message.video.file_id
        file_type = "video"
        text_content = message.caption
    elif message.document:
        file_id = message.document.file_id
        file_type = "document"
        text_content = message.caption
    
    await save_property_field(property_id, section, field_key, field_name, text_content, file_id, file_type)
    
    # Получаем заполненные поля для показа индикаторов
    filled_fields = await get_filled_fields(property_id, section)
    
    # Возвращаемся в раздел
    if section == "help":
        keyboard = get_help_subsection_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 🏠 Помощь с проживанием"
    elif section == "stores":
        keyboard = get_stores_subsection_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 📍 Магазины, аптеки итд."
    elif section == "rent":
        keyboard = get_rent_section_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 📹 Аренда"
    elif section == "experiences":
        keyboard = get_experiences_section_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 🍿 Впечатления"
    elif section == "checkout":
        keyboard = get_checkout_section_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 📦 Выселение"
    else:
        # checkin - нужно получить все заполненные поля включая подразделы
        filled_checkin = await get_filled_fields(property_id, 'checkin')
        filled_help = await get_filled_fields(property_id, 'help')
        filled_stores = await get_filled_fields(property_id, 'stores')
        all_filled = filled_checkin | filled_help | filled_stores
        
        # ИСПРАВЛЕНО: используем асинхронную функцию с await
        keyboard = await get_checkin_section_keyboard_async(property_id, all_filled)
        text = "Вы на странице категории 🧳 Заселение"
    
    await message.answer(text, reply_markup=keyboard)
    await state.clear()

@dp.callback_query(F.data.startswith("skip_field_"))
async def skip_field(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    section = parts[2]
    property_id = int(parts[3])
    
    # Получаем заполненные поля для правильного отображения индикаторов
    if section == "help":
        filled_fields = await get_filled_fields(property_id, 'help')
        keyboard = get_help_subsection_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 🏠 Помощь с проживанием"
    elif section == "stores":
        filled_fields = await get_filled_fields(property_id, 'stores')
        keyboard = get_stores_subsection_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 📍 Магазины, аптеки итд."
    elif section == "rent":
        filled_fields = await get_filled_fields(property_id, 'rent')
        keyboard = get_rent_section_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 📹 Аренда"
    elif section == "experiences":
        filled_fields = await get_filled_fields(property_id, 'experiences')
        keyboard = get_experiences_section_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 🍿 Впечатления"
    elif section == "checkout":
        filled_fields = await get_filled_fields(property_id, 'checkout')
        keyboard = get_checkout_section_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 📦 Выселение"
    else:  # checkin
        filled_checkin = await get_filled_fields(property_id, 'checkin')
        filled_help = await get_filled_fields(property_id, 'help')
        filled_stores = await get_filled_fields(property_id, 'stores')
        all_filled = filled_checkin | filled_help | filled_stores
        
        keyboard = await get_checkin_section_keyboard_async(property_id, all_filled)
        text = "Вы на странице категории 🧳 Заселение"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await state.clear()
    await callback.answer()

# Добавление кастомных кнопок
@dp.callback_query(F.data.startswith("add_custom_"))
async def add_custom_button_start(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    section = parts[2]
    property_id = int(parts[3])
    
    await state.update_data(
        custom_section=section,
        custom_property_id=property_id
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_{section}_{property_id}")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"section_{section}_{property_id}")]
    ])
    
    await callback.message.edit_text(
        "Вы редактируете кнопку\n\nВведите название кнопки:",
        reply_markup=keyboard
    )
    await state.set_state(PropertyStates.adding_custom_button_name)
    await callback.answer()

@dp.message(PropertyStates.adding_custom_button_name)
async def process_custom_button_name(message: types.Message, state: FSMContext):
    data = await state.get_data()
    custom_name = message.text
    section = data['custom_section']
    property_id = data['custom_property_id']
    
    await state.update_data(custom_button_name=custom_name)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_{section}_{property_id}")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"section_{section}_{property_id}")]
    ])
    
    await message.answer(
        "Вы редактируете кнопку\n\nВведите содержимое кнопки:",
        reply_markup=keyboard
    )
    await state.set_state(PropertyStates.adding_custom_button_content)

@dp.message(PropertyStates.adding_custom_button_content)
async def process_custom_button_content(message: types.Message, state: FSMContext):
    data = await state.get_data()
    property_id = data['custom_property_id']
    section = data['custom_section']
    field_name = data['custom_button_name']
    
    # Сохраняем контент во временное хранилище
    text_content = None
    file_id = None
    file_type = None
    
    if message.text:
        text_content = message.text
    elif message.photo:
        file_id = message.photo[-1].file_id
        file_type = "photo"
        text_content = message.caption
    elif message.video:
        file_id = message.video.file_id
        file_type = "video"
        text_content = message.caption
    elif message.document:
        file_id = message.document.file_id
        file_type = "document"
        text_content = message.caption
    
    # Сохраняем для подтверждения
    await state.update_data(
        custom_text_content=text_content,
        custom_file_id=file_id,
        custom_file_type=file_type
    )
    
    # Спрашиваем подтверждение
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"section_{section}_{property_id}")],
        [InlineKeyboardButton(text="Сохранить", callback_data=f"save_custom_{section}_{property_id}")],
        [InlineKeyboardButton(text="Не сохранять", callback_data=f"section_{section}_{property_id}")]
    ])
    
    await message.answer(
        "Сохранить кнопку?",
        reply_markup=keyboard
    )
    await state.set_state(PropertyStates.waiting_custom_confirm)

# Состояние для подтверждения сохранения
# Обработчик сохранения кастомной кнопки
@dp.callback_query(F.data.startswith("save_custom_"))
async def save_custom_field(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    section = parts[2]
    property_id = int(parts[3])
    
    data = await state.get_data()
    field_name = data['custom_button_name']
    text_content = data.get('custom_text_content')
    file_id = data.get('custom_file_id')
    file_type = data.get('custom_file_type')
    
    # Генерируем уникальный ключ для кастомной кнопки
    import time
    field_key = f"custom_{int(time.time())}_{field_name.lower().replace(' ', '_')[:20]}"
    
    # Сохраняем в БД
    await save_property_field(property_id, section, field_key, field_name, text_content, file_id, file_type)
    
    # Показываем страницу кастомной кнопки
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"section_{section}_{property_id}")],
        [InlineKeyboardButton(text="Изменить кнопку", callback_data=f"edit_custom_{property_id}_{section}_{field_key}")],
        [InlineKeyboardButton(text="Удалить кнопку", callback_data=f"delete_custom_{property_id}_{section}_{field_key}")]
    ])
    
    preview_text = text_content[:50] + "..." if text_content and len(text_content) > 50 else text_content or "(контент)"
    
    text = f"Вы на странице Новая кнопка\n\nсодержимое новой кнопки\n{preview_text}"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await state.clear()
    await callback.answer("✅ Кнопка сохранена!")

# Обработчик удаления кастомной кнопки
@dp.callback_query(F.data.startswith("delete_custom_"))
async def delete_custom_field_handler(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    property_id = int(parts[2])
    section = parts[3]
    field_key = "_".join(parts[4:])
    
    # Удаляем из БД
    await delete_custom_field(property_id, section, field_key)
    
    # Возвращаемся в раздел
    filled_fields = await get_filled_fields(property_id, section)
    
    if section == "checkin":
        filled_checkin = await get_filled_fields(property_id, 'checkin')
        filled_help = await get_filled_fields(property_id, 'help')
        filled_stores = await get_filled_fields(property_id, 'stores')
        all_filled = filled_checkin | filled_help | filled_stores
        keyboard = await get_checkin_section_keyboard_async(property_id, all_filled)
        text = "Вы на странице категории 🧳 Заселение"
    elif section == "rent":
        keyboard = get_rent_section_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 📹 Аренда"
    elif section == "experiences":
        keyboard = get_experiences_section_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 🍿 Впечатления"
    elif section == "checkout":
        keyboard = get_checkout_section_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 📦 Выселение"
    elif section == "help":
        keyboard = get_help_subsection_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 🏠 Помощь с проживанием"
    else:  # stores
        keyboard = get_stores_subsection_keyboard(property_id, filled_fields)
        text = "Вы на странице категории 📍 Магазины, аптеки итд."
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer("✅ Кнопка удалена!")

# Обработчик просмотра/редактирования кастомной кнопки
@dp.callback_query(F.data.startswith("custom_field_"))
async def view_custom_field(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    property_id = int(parts[2])
    section = parts[3]
    field_key = "_".join(parts[4:])
    
    # Получаем данные кнопки
    field_data = await get_property_field(property_id, section, field_key)
    
    if not field_data:
        await callback.answer("Кнопка не найдена", show_alert=True)
        return
    
    # Показываем страницу кнопки
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"section_{section}_{property_id}")],
        [InlineKeyboardButton(text="Изменить кнопку", callback_data=f"edit_custom_{property_id}_{section}_{field_key}")],
        [InlineKeyboardButton(text="Удалить кнопку", callback_data=f"delete_custom_{property_id}_{section}_{field_key}")]
    ])
    
    text_content = field_data['text_content']
    preview_text = text_content[:50] + "..." if text_content and len(text_content) > 50 else text_content or "(контент)"
    
    # Получаем название кнопки
    custom_fields = await get_custom_fields(property_id, section)
    button_name = next((f['field_name'] for f in custom_fields if f['field_key'] == field_key), "Кнопка")
    
    text = f"Вы на странице {button_name}\n\nсодержимое новой кнопки\n{preview_text}"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# Бронирования
@dp.callback_query(F.data.startswith("bookings_"))
async def bookings_menu(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[1])
    
    bookings = await get_property_bookings(property_id)
    
    text = (
        "Ниже перечислены ваши бронирования. Бронь необходимо выдавать гостю, чтобы он мог получить доступ к закрытой "
        "информации для вашего объекта. Например, информацию о коде для сейфа.\n\n"
        "После проживания бронирование нужно завершить."
    )
    
    buttons = []
    
    for booking in bookings:
        guest_name = booking['guest_name']
        checkin = booking['checkin_date'].strftime('%d.%m.%y')
        icon = "🔴" if booking['is_active'] else "⚪"
        buttons.append([InlineKeyboardButton(
            text=f"{guest_name} — {checkin} {icon}",
            callback_data=f"view_booking_{booking['id']}"
        )])
    
    buttons.append([InlineKeyboardButton(text="➕ Добавить бронирование", callback_data=f"add_booking_{property_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"property_{property_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("add_booking_"))
async def add_booking(callback: types.CallbackQuery, state: FSMContext):
    property_id = int(callback.data.split("_")[2])
    
    await state.update_data(booking_property_id=property_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"bookings_{property_id}")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"bookings_{property_id}")]
    ])
    
    await callback.message.edit_text(
        "Вы редактируете кнопку\n\nВведите ФИО гостя:",
        reply_markup=keyboard
    )
    await state.set_state(BookingStates.waiting_guest_name)
    await callback.answer()

@dp.message(BookingStates.waiting_guest_name)
async def process_guest_name(message: types.Message, state: FSMContext):
    await state.update_data(guest_name=message.text)
    
    data = await state.get_data()
    property_id = data.get('booking_property_id')
    
    # ИСПРАВЛЕНИЕ: Защита от потери state
    if not property_id:
        await state.clear()
        await message.answer(
            "❌ Произошла ошибка. Попробуйте создать бронирование заново.",
            reply_markup=get_main_menu_keyboard()
        )
        logger.warning(f"Lost booking_property_id in state for user {message.from_user.id}")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"bookings_{property_id}")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"bookings_{property_id}")]
    ])
    
    await message.answer(
        "Введите дату заезда в формате 20.06.2025",
        reply_markup=keyboard
    )
    await state.set_state(BookingStates.waiting_checkin_date)

@dp.message(BookingStates.waiting_checkin_date)
async def process_checkin_date(message: types.Message, state: FSMContext):
    data = await state.get_data()
    property_id = data.get('booking_property_id')
    guest_name = data.get('guest_name')
    
    # ИСПРАВЛЕНИЕ: Защита от потери state
    if not property_id or not guest_name:
        await state.clear()
        await message.answer(
            "❌ Произошла ошибка. Попробуйте создать бронирование заново.",
            reply_markup=get_main_menu_keyboard()
        )
        logger.warning(f"Lost booking data in state for user {message.from_user.id}")
        return
    
    try:
        checkin_date = datetime.strptime(message.text, '%d.%m.%Y').date()
        booking_id, access_code = await create_booking(property_id, guest_name, checkin_date)
        
        bot_username = (await bot.get_me()).username
        guest_link = f"https://t.me/{bot_username}?start=guest_{access_code}"
        
        text = (
            "Ниже перечислены ваши бронирования. Бронь необходимо выдавать гостю, чтобы он мог получить доступ к закрытой "
            "информации для вашего объекта. Например, информацию о коде для сейфа.\n\n"
            "После проживания бронирование нужно завершить."
        )
        
        bookings = await get_property_bookings(property_id)
        
        buttons = []
        for booking in bookings:
            b_guest_name = booking['guest_name']
            b_checkin = booking['checkin_date'].strftime('%d.%m.%y')
            icon = "🔴" if booking['is_active'] else "⚪"
            buttons.append([InlineKeyboardButton(
                text=f"{b_guest_name} — {b_checkin} {icon}",
                callback_data=f"view_booking_{booking['id']}"
            )])
        
        buttons.append([InlineKeyboardButton(text="➕ Добавить бронирование", callback_data=f"add_booking_{property_id}")])
        buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"property_{property_id}")])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await message.answer(text, reply_markup=keyboard)
        await message.answer(f"🔗 Ссылка для гостя:\n{guest_link}")
        await state.clear()
        
    except ValueError:
        await message.answer("Неверный формат даты. Используйте формат: 20.06.2025")

@dp.callback_query(F.data.startswith("view_booking_"))
async def view_booking(callback: types.CallbackQuery):
    booking_id = int(callback.data.split("_")[2])
    
    async with db_pool.acquire() as conn:
        property_id = await conn.fetchval('SELECT property_id FROM bookings WHERE id = $1', booking_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Завершить бронирование", callback_data=f"complete_booking_{booking_id}_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"bookings_{property_id}")]
    ])
    
    await callback.message.edit_text("Детали бронирования", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("complete_booking_"))
async def complete_booking_handler(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    booking_id = int(parts[2])
    property_id = int(parts[3]) if len(parts) > 3 else None
    
    await complete_booking(booking_id)
    
    if property_id:
        bookings = await get_property_bookings(property_id)
        
        text = (
            "Бронирование завершено.\n\n"
            "Ниже перечислены ваши бронирования. Бронь необходимо выдавать гостю, чтобы он мог получить доступ к закрытой "
            "информации для вашего объекта. Например, информацию о коде для сейфа.\n\n"
            "После проживания бронирование нужно завершить."
        )
        
        buttons = []
        for booking in bookings:
            guest_name = booking['guest_name']
            checkin = booking['checkin_date'].strftime('%d.%m.%y')
            icon = "🔴" if booking['is_active'] else "⚪"
            buttons.append([InlineKeyboardButton(
                text=f"{guest_name} — {checkin} {icon}",
                callback_data=f"view_booking_{booking['id']}"
            )])
        
        buttons.append([InlineKeyboardButton(text="➕ Добавить бронирование", callback_data=f"add_booking_{property_id}")])
        buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"property_{property_id}")])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.edit_text(text, reply_markup=keyboard)
    else:
        await callback.message.edit_text("Бронирование завершено")
    
    await callback.answer("Бронирование завершено")

# Предпросмотр объекта (как гость)
@dp.callback_query(F.data.startswith("prop_preview_"))
async def preview_property(callback: types.CallbackQuery, state: FSMContext):
    property_id = int(callback.data.split("_")[2])
    
    await state.update_data(preview_mode=True, preview_property_id=property_id)
    
    property_name = await get_property_name(property_id)
    address = await get_property_address(property_id) or "МОСква"
    
    text = f"{property_name}\n\nАдрес апартаментов: {address}.\n\nВот информация, доступная для изучения:"
    
    # Получаем доступные разделы
    sections_data = await get_property_sections_data(property_id)
    available_sections = set(row['section'] for row in sections_data)
    
    buttons = []
    buttons.append([InlineKeyboardButton(text="➡️ Начать", callback_data=f"prevw_start_{property_id}")])
    buttons.append([InlineKeyboardButton(text="🚕 Вызвать такси", url="https://taxi.yandex.ru")])
    buttons.append([InlineKeyboardButton(text="Переключится в режим владельца бота", callback_data=f"exit_preview_{property_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# Старт предпросмотра (нажатие "Начать")
@dp.callback_query(F.data.startswith("prevw_start_"))
async def preview_start(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    
    property_name = await get_property_name(property_id)
    
    # Получаем доступные разделы
    sections_data = await get_property_sections_data(property_id)
    available_sections = set(row['section'] for row in sections_data)
    
    buttons = []
    if 'rent' in available_sections:
        buttons.append([InlineKeyboardButton(text="📹 Аренда", callback_data=f"prevw_section_rent_{property_id}")])
    if 'checkin' in available_sections:
        buttons.append([InlineKeyboardButton(text="🧳 Заселение", callback_data=f"prevw_section_checkin_{property_id}")])
    if 'experiences' in available_sections:
        buttons.append([InlineKeyboardButton(text="🍿 Впечатления", callback_data=f"prevw_section_experiences_{property_id}")])
    if 'checkout' in available_sections:
        buttons.append([InlineKeyboardButton(text="📦 Выселение", callback_data=f"prevw_section_checkout_{property_id}")])
    
    buttons.append([InlineKeyboardButton(text="Переключится в режим владельца бота", callback_data=f"exit_preview_{property_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    text = f"{property_name}\n\nВот информация, доступная для изучения:"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# Просмотр раздела в предпросмотре
@dp.callback_query(F.data.startswith("prevw_section_"))
async def preview_section(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    section = parts[2]
    property_id = int(parts[3])
    
    fields = await get_section_fields(property_id, section)
    
    if not fields:
        await callback.answer("В этом разделе пока нет информации", show_alert=True)
        return
    
    section_name = SECTION_NAMES.get(section, section)
    section_icon = SECTION_ICONS.get(section, "📄")
    
    # Добавляем сердечко к разделу Заселение как на скриншоте
    if section == 'checkin':
        text = f"Вы на странице категории {section_icon} {section_name} ❤️"
    else:
        text = f"Вы на странице категории {section_icon} {section_name}"
    
    buttons = []
    for field in fields:
        field_name = field['field_name']
        buttons.append([InlineKeyboardButton(text=field_name, callback_data=f"prevw_field_{property_id}_{section}_{field['field_key']}")])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"prevw_start_{property_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# Просмотр поля в предпросмотре
@dp.callback_query(F.data.startswith("prevw_field_"))
async def preview_field(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    property_id = int(parts[2])
    section = parts[3]
    field_key = "_".join(parts[4:])
    
    field_data = await get_property_field(property_id, section, field_key)
    
    if not field_data:
        await callback.answer("Нет данных для этого поля", show_alert=True)
        return
    
    # Получаем название поля с иконкой
    field_name = FIELD_NAMES.get(field_key, "Поле")
    
    text_content = field_data['text_content']
    file_id = field_data['file_id']
    file_type = field_data['file_type']
    
    # Формируем заголовок
    header = f"Вы на странице {field_name}"
    
    # Кнопка "Назад"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"prevw_section_{section}_{property_id}")]
    ])
    
    # Если есть файл - отправляем с caption
    if file_id:
        try:
            caption = f"{header}\n\n{text_content}" if text_content else header
            
            if file_type == "photo":
                await callback.message.delete()
                await callback.message.answer_photo(file_id, caption=caption, reply_markup=keyboard)
            elif file_type == "video":
                await callback.message.delete()
                await callback.message.answer_video(file_id, caption=caption, reply_markup=keyboard)
            elif file_type == "document":
                await callback.message.delete()
                await callback.message.answer_document(file_id, caption=caption, reply_markup=keyboard)
        except Exception as e:
            logger.error(f"Error sending media: {e}")
            # Если не удалось отправить медиа, отправляем текстом
            full_text = f"{header}\n\n{text_content}" if text_content else header
            await callback.message.edit_text(full_text, reply_markup=keyboard)
    elif text_content:
        # Только текст - показываем в одном сообщении
        full_text = f"{header}\n\n{text_content}"
        await callback.message.edit_text(full_text, reply_markup=keyboard)
    else:
        # Нет контента
        await callback.message.edit_text(header, reply_markup=keyboard)
    
    await callback.answer()

# Выход из предпросмотра
@dp.callback_query(F.data.startswith("exit_preview_"))
async def exit_preview(callback: types.CallbackQuery, state: FSMContext):
    property_id = int(callback.data.split("_")[2])
    await state.update_data(preview_mode=False)
    
    property_name = await get_property_name(property_id)
    text = f"Вы на странице объекта {property_name}.\n\nТут вы можете отредактировать информацию о объекте, которая будет доступна гостям."
    await callback.message.edit_text(text, reply_markup=get_property_menu_keyboard(property_id))
    await callback.answer()

# Удаление объекта
@dp.callback_query(F.data.startswith("delete_property_"))
async def confirm_delete_property(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_delete_{property_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"property_{property_id}")]
    ])
    
    await callback.message.edit_text("Вы точно хотите удалить объект?", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("confirm_delete_"))
async def delete_property_confirmed(callback: types.CallbackQuery, state: FSMContext):
    property_id = int(callback.data.split("_")[2])
    await delete_property(property_id)
    
    data = await state.get_data()
    company_id = data.get('current_company_id')
    properties = await get_company_properties(company_id)
    
    await callback.message.edit_text(
        "Объект удален.\n\nВот список ваших объектов:",
        reply_markup=get_objects_list_keyboard(properties)
    )
    await callback.answer("Объект удален")

# Переключение долгосрок/краткосрок
@dp.callback_query(F.data.startswith("toggle_shortterm_"))
async def toggle_shortterm_handler(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    await toggle_short_term(property_id)
    
    # Получаем обновленную информацию
    property_info = await get_property_info(property_id)
    property_name = property_info['name']
    is_short_term = property_info['is_short_term']
    
    # Текст уведомления
    mode_text = "краткосрочная аренда" if is_short_term else "долгосрочная аренда"
    
    text = f"Вы на странице объекта {property_name}.\n\nТут вы можете отредактировать информацию о объекте, которая будет доступна гостям."
    await callback.message.edit_text(text, reply_markup=get_property_menu_keyboard(property_id, is_short_term))
    await callback.answer(f"✅ Режим изменен на {mode_text}")

# Ссылка для владельца объекта
@dp.callback_query(F.data.startswith("owner_link_"))
async def generate_owner_link(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    property_name = await get_property_name(property_id)
    
    bot_username = (await bot.get_me()).username
    owner_link = f"https://t.me/{bot_username}?start=owner_{property_id}"
    
    text = f"Ссылка для приглашения менеджера в компанию, по-умолчанию менеджер не может удалять объекты:\n{owner_link}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"property_{property_id}")]
    ])
    
    await callback.message.answer(text, reply_markup=keyboard)
    await callback.answer()

# Режим гостя
@dp.callback_query(F.data.startswith("guest_start_"))
async def guest_start(callback: types.CallbackQuery, state: FSMContext):
    property_id = int(callback.data.split("_")[2])
    
    await state.update_data(guest_mode=True, guest_property_id=property_id)
    
    property_name = await get_property_name(property_id)
    
    sections_data = await get_property_sections_data(property_id)
    available_sections = set(row['section'] for row in sections_data)
    
    buttons = []
    if 'rent' in available_sections:
        buttons.append([InlineKeyboardButton(text="📹 Аренда", callback_data=f"guest_section_rent_{property_id}")])
    if 'checkin' in available_sections:
        buttons.append([InlineKeyboardButton(text="🧳 Заселение", callback_data=f"guest_section_checkin_{property_id}")])
    if 'experiences' in available_sections:
        buttons.append([InlineKeyboardButton(text="🍿 Впечатления", callback_data=f"guest_section_experiences_{property_id}")])
    if 'checkout' in available_sections:
        buttons.append([InlineKeyboardButton(text="📦 Выселение", callback_data=f"guest_section_checkout_{property_id}")])
    
    buttons.append([InlineKeyboardButton(text="Переключится в режим владельца бота", callback_data="switch_to_owner")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    text = f"{property_name}\n\nВот информация, доступная для изучения:"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("guest_section_"))
async def guest_view_section(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    section = parts[2]
    property_id = int(parts[3])
    
    fields = await get_section_fields(property_id, section)
    
    if not fields:
        await callback.answer("В этом разделе пока нет информации", show_alert=True)
        return
    
    section_name = SECTION_NAMES.get(section, section)
    section_icon = SECTION_ICONS.get(section, "📄")
    
    # Добавляем сердечко к разделу Заселение как на скриншоте
    if section == 'checkin':
        text = f"Вы на странице категории {section_icon} {section_name} ❤️"
    else:
        text = f"Вы на странице категории {section_icon} {section_name}"
    
    buttons = []
    for field in fields:
        field_name = field['field_name']
        buttons.append([InlineKeyboardButton(text=field_name, callback_data=f"guest_field_{property_id}_{section}_{field['field_key']}")])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"guest_start_{property_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("guest_field_"))
async def guest_view_field(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    property_id = int(parts[2])
    section = parts[3]
    field_key = "_".join(parts[4:])
    
    field_data = await get_property_field(property_id, section, field_key)
    
    if not field_data:
        await callback.answer("Нет данных для этого поля", show_alert=True)
        return
    
    # Получаем название поля с иконкой
    field_name = FIELD_NAMES.get(field_key, "Поле")
    
    text_content = field_data['text_content']
    file_id = field_data['file_id']
    file_type = field_data['file_type']
    
    # Формируем заголовок
    header = f"Вы на странице {field_name}"
    
    # Кнопка "Назад"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"guest_section_{section}_{property_id}")]
    ])
    
    # Если есть файл - отправляем с caption
    if file_id:
        try:
            caption = f"{header}\n\n{text_content}" if text_content else header
            
            if file_type == "photo":
                await callback.message.delete()
                await callback.message.answer_photo(file_id, caption=caption, reply_markup=keyboard)
            elif file_type == "video":
                await callback.message.delete()
                await callback.message.answer_video(file_id, caption=caption, reply_markup=keyboard)
            elif file_type == "document":
                await callback.message.delete()
                await callback.message.answer_document(file_id, caption=caption, reply_markup=keyboard)
        except Exception as e:
            logger.error(f"Error sending media: {e}")
            # Если не удалось отправить медиа, отправляем текстом
            full_text = f"{header}\n\n{text_content}" if text_content else header
            await callback.message.edit_text(full_text, reply_markup=keyboard)
    elif text_content:
        # Только текст - показываем в одном сообщении
        full_text = f"{header}\n\n{text_content}"
        await callback.message.edit_text(full_text, reply_markup=keyboard)
    else:
        # Нет контента
        await callback.message.edit_text(header, reply_markup=keyboard)
    
    await callback.answer()

@dp.callback_query(F.data == "switch_to_owner")
async def switch_to_owner_mode(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    
    companies = await get_user_companies(callback.from_user.id)
    
    if companies:
        await state.update_data(current_company_id=companies[0][0])
        text = (
            "Вы в главном меню бота 🏠\n\n"
            "Если вы хотите добавить апартаменты и поделиться ссылкой с гостями, "
            "переходите в раздел «Добавление и настройка объектов»"
        )
        await callback.message.edit_text(text, reply_markup=get_main_menu_keyboard())
    else:
        await callback.message.edit_text(
            "Для работы в режиме владельца создайте компанию",
            reply_markup=get_add_company_keyboard()
        )
    
    await callback.answer("Переключено в режим владельца")

# ============================================
# СИСТЕМА ПРЕДЛОЖЕНИЙ ПО УЛУЧШЕНИЮ БОТА
# ============================================

@dp.callback_query(F.data == "suggest_improvement")
async def suggest_improvement_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало процесса отправки предложения"""
    user_id = callback.from_user.id
    
    # Проверяем rate limiting (защита от спама)
    recent_count = await get_recent_suggestions_count(user_id, hours=1)
    
    if recent_count >= 3:
        await callback.answer(
            "⚠️ Вы отправили слишком много предложений. Пожалуйста, подождите час.",
            show_alert=True
        )
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])
    
    text = (
        "💡 Предложить улучшение бота\n\n"
        "Мы ценим ваше мнение! Напишите, что бы вы хотели улучшить в боте:\n\n"
        "• Новые функции\n"
        "• Исправления\n"
        "• Идеи по улучшению интерфейса\n\n"
        "Ваше предложение будет отправлено разработчикам."
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await state.set_state(SuggestionStates.waiting_suggestion)
    await callback.answer()

@dp.message(SuggestionStates.waiting_suggestion)
async def process_suggestion(message: types.Message, state: FSMContext):
    """Обработка полученного предложения"""
    user_id = message.from_user.id
    username = message.from_user.username or "Без username"
    first_name = message.from_user.first_name or "Без имени"
    suggestion_text = message.text
    
    # Проверяем длину предложения
    if len(suggestion_text) < 10:
        await message.answer(
            "⚠️ Предложение слишком короткое. Пожалуйста, напишите подробнее (минимум 10 символов).",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Отмена", callback_data="main_menu")]
            ])
        )
        return
    
    if len(suggestion_text) > 1000:
        await message.answer(
            "⚠️ Предложение слишком длинное. Пожалуйста, сократите текст (максимум 1000 символов).",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Отмена", callback_data="main_menu")]
            ])
        )
        return
    
    # Сохраняем предложение в БД
    suggestion_id = await save_suggestion(user_id, username, first_name, suggestion_text)
    
    # Отправляем предложение всем админам
    admins = await get_bot_admins()
    sent_count = 0
    
    for admin in admins:
        try:
            admin_message = (
                f"📩 <b>Новое предложение #{suggestion_id}</b>\n\n"
                f"👤 <b>От:</b> {first_name} (@{username})\n"
                f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
                f"📅 <b>Дата:</b> {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
                f"💬 <b>Предложение:</b>\n{suggestion_text}"
            )
            
            await bot.send_message(
                admin['user_id'],
                admin_message,
                parse_mode="HTML"
            )
            sent_count += 1
        except Exception as e:
            logger.error(f"Failed to send suggestion to admin {admin['user_id']}: {e}")
    
    logger.info(f"Suggestion #{suggestion_id} from user {user_id} sent to {sent_count} admins")
    
    # Благодарим пользователя
    await message.answer(
        "✅ Спасибо за предложение!\n\n"
        "Ваше сообщение отправлено разработчикам. Мы обязательно рассмотрим его.",
        reply_markup=get_main_menu_keyboard()
    )
    
    await state.clear()

# Запуск бота
async def on_shutdown():
    logger.info("Shutting down...")
    if db_pool:
        await db_pool.close()
    await bot.session.close()

async def delete_webhook_and_prepare():
    """Удаляет webhook и подготавливает бота к polling режиму"""
    try:
        # Удаляем webhook если он был установлен
        webhook_info = await bot.get_webhook_info()
        if webhook_info.url:
            logger.info(f"Removing existing webhook: {webhook_info.url}")
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook removed successfully")
        else:
            logger.info("ℹ️  No webhook set, proceeding with polling")
        
        # Даём время старому боту завершиться
        await asyncio.sleep(2)
        
    except Exception as e:
        logger.warning(f"⚠️ Error while preparing bot: {e}")

async def main():
    try:
        await init_db()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return
    
    logger.info("Bot started successfully")
    
    # HTTP сервер для health checks - ОБЯЗАТЕЛЕН для Railway
    port = int(os.getenv("PORT", "8080"))
    
    from aiohttp import web
    
    async def health_check(request):
        return web.Response(text="Bot is running")
    
    async def readiness_check(request):
        try:
            async with db_pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
            return web.Response(text="Ready", status=200)
        except Exception as e:
            logger.error(f"Readiness check failed: {e}")
            return web.Response(text="Not ready", status=503)
    
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    app.router.add_get("/ready", readiness_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    logger.info(f"Starting health check server on port {port}")
    await site.start()
    
    # Удаляем webhook и готовимся к polling
    await delete_webhook_and_prepare()
    
    # Настройка graceful shutdown
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()
    
    def signal_handler():
        logger.info("🛑 Received shutdown signal, stopping bot gracefully...")
        shutdown_event.set()
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)
    
    # Установка команд бота для отображения в меню Telegram
    from aiogram.types import BotCommand
    
    commands = [
        BotCommand(command="start", description="🚀 Запустить бота"),
        BotCommand(command="menu", description="🏠 Главное меню"),
        BotCommand(command="company", description="🏢 Личный кабинет компании"),
        BotCommand(command="apartments", description="🏘️ Список объектов")
    ]
    
    try:
        await bot.set_my_commands(commands)
        logger.info("✅ Bot commands set successfully")
    except Exception as e:
        logger.error(f"⚠️ Failed to set bot commands: {e}")
    
    # Запуск polling с graceful shutdown
    polling_task = None
    try:
        logger.info("🚀 Starting bot polling...")
        
        # Создаем задачу polling
        polling_task = asyncio.create_task(
            dp.start_polling(
                bot,
                allowed_updates=dp.resolve_used_update_types(),
                drop_pending_updates=True
            )
        )
        
        # Ждём или сигнала завершения, или завершения polling
        done, pending = await asyncio.wait(
            [polling_task, asyncio.create_task(shutdown_event.wait())],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Если получили shutdown signal - останавливаем polling
        if not polling_task.done():
            logger.info("⏹️ Stopping polling gracefully...")
            polling_task.cancel()
            try:
                await polling_task
            except asyncio.CancelledError:
                logger.info("✅ Polling stopped successfully")
        
    except Exception as e:
        logger.error(f"❌ Polling error: {e}")
    finally:
        logger.info("🧹 Cleaning up resources...")
        await on_shutdown()
        await runner.cleanup()
        logger.info("👋 Bot stopped successfully")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped by user")
