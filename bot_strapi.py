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
from typing import Optional, Dict, List, Tuple
import secrets
import hashlib

# ============================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# КОНФИГУРАЦИЯ
# ============================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Глобальный пул соединений
db_pool: Optional[asyncpg.Pool] = None

# ============================================
# КОНСТАНТЫ ДЛЯ МАППИНГА РАЗДЕЛОВ И КАТЕГОРИЙ
# ============================================

# Маппинг разделов бота на категории в БД
SECTION_TO_CATEGORY_MAP = {
    'checkin': 'Заселение',
    'rent': 'Аренда', 
    'experiences': 'Впечатления',
    'checkout': 'Выселение',
    'help': 'Помощь с проживанием',
    'stores': 'Магазины и услуги'
}

# Маппинг полей на категории
FIELD_TO_CATEGORY_MAP = {
    'checkin_time': 'Время заселения',
    'parking': 'Парковка',
    'wifi': 'Wi-Fi',
    'door_key': 'Ключ от двери',
    'how_to_find': 'Как найти объект',
    'how_to_reach': 'Как дойти до квартиры',
    'documents': 'Документы',
    'deposit': 'Депозит',
    'remote_checkin': 'Дистанционное заселение',
    'rules': 'Правила проживания',
    'breakfast': 'Завтрак',
    'linen': 'Смена белья',
    'manager_contact': 'Связь с менеджером',
    'tv_setup': 'Настройка ТВ',
    'ac': 'Кондиционер',
    'shops': 'Магазины',
    'car_rental': 'Аренда автомобилей',
    'sport': 'Спорт',
    'hospitals': 'Больницы',
    'uk_phones': 'Телефоны УК',
    'dispatcher': 'Диспетчер',
    'emergency': 'Аварийная служба',
    'chats': 'Домовые чаты',
    'feedback_form': 'Обратная связь',
    'internet': 'Интернет',
    'excursions': 'Экскурсии',
    'museums': 'Музеи',
    'parks': 'Парки',
    'entertainment': 'Развлечения',
    'self_checkout': 'Самостоятельный выезд',
    'deposit_return': 'Возврат депозита',
    'extend_stay': 'Продление',
    'discounts': 'Скидки'
}

# Иконки для UI
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
    'how_to_find': 'Здесь вы можете рассказать, с какой стороны подъехать к вашему подъезду',
    'how_to_reach': 'Покажите процесс как добраться до квартиры',
    'documents': 'Здесь вы можете прикрепить необходимые документы',
    'deposit': 'Здесь вы можете добавить информацию о залоге',
    'remote_checkin': 'Расскажите, как проходит дистанционное заселение',
    'rules': 'Здесь вы можете добавить правила проживания',
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

# URL-ссылки для полезных функций
USEFUL_LINKS = {
    'new_apartment': 'https://t.me/c/1866133787/28060/119241',
    'accountant': 'https://t.me/c/1866133787/28048/103192',
    'group_purchase': 'https://t.me/c/1866133787/28054/87121',
    'guest_exchange': 'https://t.me/c/1866133787/36297/36312',
    'lawyer': 'https://t.me/c/1866133787/28051/83480',
    'location': 'https://t.me/c/1866133787/42660/87001',
    'furnish': 'https://t.me/c/1866133787/28052/102033',
    'invest': 'https://t.me/c/1866133787/28056/102632',
    'books': 'https://t.me/c/1866133787/75904/88764',
    'psychology': 'https://t.me/c/1866133787/28058/99597',
    'join_chat': 'https://t.me/mir_any'
}

# ============================================
# MIDDLEWARE
# ============================================

@dp.update.outer_middleware()
async def auto_register_manager_middleware(handler, event: types.Update, data: dict):
    """
    Автоматически регистрирует менеджера в БД при взаимодействии с ботом.
    В Strapi БД нет таблицы users - работаем только с managers.
    """
    user = None
    
    if event.message:
        user = event.message.from_user
    elif event.callback_query:
        user = event.callback_query.from_user
    elif event.inline_query:
        user = event.inline_query.from_user
    
    if user and db_pool:
        try:
            telegram_id_str = str(user.id)
            
            async with db_pool.acquire() as conn:
                # Проверяем существует ли менеджер
                exists = await conn.fetchval(
                    'SELECT 1 FROM managers WHERE telegram_id = $1',
                    telegram_id_str
                )
                
                if not exists:
                    # Создаём менеджера без организации
                    await conn.execute('''
                        INSERT INTO managers (
                            telegram_id, name, lastname, 
                            created_at, updated_at, published_at
                        )
                        VALUES ($1, $2, $3, NOW(), NOW(), NOW())
                    ''', telegram_id_str, user.first_name, user.username or '')
                    
        except Exception as e:
            logger.error(f"⚠️ Error auto-registering manager {user.id}: {e}")
    
    return await handler(event, data)

# ============================================
# ERROR HANDLERS
# ============================================

@dp.error()
async def global_error_handler(event: types.ErrorEvent):
    """Глобальный обработчик ошибок"""
    logger.error(
        f"❌ Critical error during update {event.update.update_id} processing:\n"
        f"Exception: {event.exception}\n"
        f"Update: {event.update}"
    )
    
    if event.update.callback_query:
        try:
            await event.update.callback_query.answer(
                "⚠️ Произошла ошибка. Попробуйте ещё раз.",
                show_alert=True
            )
        except Exception as e:
            logger.error(f"Failed to answer callback query: {e}")
    
    elif event.update.message:
        try:
            await event.update.message.answer(
                "⚠️ Произошла ошибка. Используйте /start",
                reply_markup=get_main_menu_keyboard()
            )
        except Exception as e:
            logger.error(f"Failed to send error message: {e}")
    
    return True

# ============================================
# HELPER FUNCTIONS
# ============================================

async def clear_state_keep_company(state: FSMContext):
    """Очищает state, но сохраняет current_organization_id"""
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    await state.clear()
    if org_id:
        await state.update_data(current_organization_id=org_id)

def generate_hash() -> str:
    """Генерирует уникальный hash для organization или booking"""
    return hashlib.md5(secrets.token_bytes(32)).hexdigest()[:16]

def telegram_id_to_str(telegram_id: int) -> str:
    """Конвертирует Telegram ID в строку для БД"""
    return str(telegram_id)

# ============================================
# ИНИЦИАЛИЗАЦИЯ БД
# ============================================

async def init_db():
    """
    Инициализация подключения к БД.
    НЕ создаём таблицы - они уже существуют в Strapi.
    Проверяем и создаём необходимые базовые категории.
    """
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=20)
    
    logger.info("✅ Database pool created")
    
    # Создаём базовые категории если их нет
    async with db_pool.acquire() as conn:
        try:
            # Проверяем и создаём родительские категории (разделы)
            for section_key, section_name in SECTION_TO_CATEGORY_MAP.items():
                exists = await conn.fetchval(
                    'SELECT id FROM categories WHERE name = $1',
                    section_name
                )
                
                if not exists:
                    await conn.execute('''
                        INSERT INTO categories (
                            name, expandable, editable, 
                            created_at, updated_at, published_at
                        )
                        VALUES ($1, TRUE, TRUE, NOW(), NOW(), NOW())
                    ''', section_name)
                    logger.info(f"✅ Created category: {section_name}")
            
            logger.info("✅ Base categories verified")
            
        except Exception as e:
            logger.error(f"❌ Error creating base categories: {e}")
    
    logger.info("✅ Database initialized successfully")

# ============================================
# FSM STATES
# ============================================

class OrganizationStates(StatesGroup):
    waiting_name = State()
    waiting_city = State()
    editing_name = State()
    editing_city = State()
    editing_greeting = State()
    waiting_timezone = State()
    waiting_checkin_time = State()
    waiting_checkout_time = State()

class ApartmentStates(StatesGroup):
    waiting_name = State()
    waiting_address = State()
    editing_field = State()
    adding_custom_button_name = State()
    adding_custom_button_content = State()
    waiting_custom_confirm = State()
    editing_name = State()
    editing_address = State()

class BookingStates(StatesGroup):
    waiting_guest_name = State()
    waiting_checkin_date = State()

class SuggestionStates(StatesGroup):
    waiting_suggestion = State()

# ============================================
# DATABASE FUNCTIONS - ORGANIZATIONS
# ============================================

async def get_manager_organizations(telegram_id: int) -> List[Tuple[int, str, str]]:
    """Получить список организаций менеджера"""
    telegram_id_str = telegram_id_to_str(telegram_id)
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT o.id, o.name, o.city
            FROM organizations o
            JOIN managers_organization_lnk mol ON o.id = mol.organization_id
            JOIN managers m ON mol.manager_id = m.id
            WHERE m.telegram_id = $1
            ORDER BY o.created_at DESC
        ''', telegram_id_str)
        
        return [(row['id'], row['name'], row['city']) for row in rows]

async def create_organization(name: str, city: str, telegram_id: int) -> int:
    """Создать новую организацию"""
    telegram_id_str = telegram_id_to_str(telegram_id)
    greeting = "Добрый день! Добро пожаловать! Вы находитесь в боте-помощнике для ваших апартаментов."
    hash_code = generate_hash()
    
    async with db_pool.acquire() as conn:
        # Получаем ID менеджера
        manager_id = await conn.fetchval(
            'SELECT id FROM managers WHERE telegram_id = $1',
            telegram_id_str
        )
        
        if not manager_id:
            # Создаём менеджера если не существует
            manager_id = await conn.fetchval('''
                INSERT INTO managers (
                    telegram_id, name, is_admin, is_owner,
                    created_at, updated_at, published_at
                )
                VALUES ($1, $2, TRUE, TRUE, NOW(), NOW(), NOW())
                RETURNING id
            ''', telegram_id_str, name)
        
        # Создаём организацию
        org_id = await conn.fetchval('''
            INSERT INTO organizations (
                name, city, greeting, hash,
                check_in, check_out, timezone, is_long,
                created_at, updated_at, published_at
            )
            VALUES ($1, $2, $3, $4, '14:00', '12:00', 'UTC+3', FALSE, NOW(), NOW(), NOW())
            RETURNING id
        ''', name, city, greeting, hash_code)
        
        # Связываем менеджера с организацией
        await conn.execute('''
            INSERT INTO managers_organization_lnk (manager_id, organization_id)
            VALUES ($1, $2)
        ''', manager_id, org_id)
        
        logger.info(f"✅ Created organization {org_id} for manager {telegram_id}")
        return org_id

async def get_organization_info(org_id: int) -> Optional[Dict]:
    """Получить информацию об организации"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT id, name, city, greeting, timezone, 
                   check_in, check_out, is_long, hash
            FROM organizations 
            WHERE id = $1
        ''', org_id)
        
        return dict(row) if row else None

async def update_organization_field(org_id: int, field: str, value):
    """Обновить поле организации"""
    allowed_fields = {'name', 'city', 'greeting', 'timezone', 'check_in', 'check_out', 'is_long'}
    
    if field not in allowed_fields:
        raise ValueError(f"Invalid field: {field}")
    
    async with db_pool.acquire() as conn:
        query = f"UPDATE organizations SET {field} = $1, updated_at = NOW() WHERE id = $2"
        await conn.execute(query, value, org_id)

async def join_organization_by_hash(telegram_id: int, hash_code: str) -> Optional[int]:
    """Присоединиться к организации по hash"""
    telegram_id_str = telegram_id_to_str(telegram_id)
    
    async with db_pool.acquire() as conn:
        org_id = await conn.fetchval(
            'SELECT id FROM organizations WHERE hash = $1',
            hash_code
        )
        
        if not org_id:
            return None
        
        # Получаем или создаём менеджера
        manager_id = await conn.fetchval(
            'SELECT id FROM managers WHERE telegram_id = $1',
            telegram_id_str
        )
        
        if not manager_id:
            manager_id = await conn.fetchval('''
                INSERT INTO managers (
                    telegram_id, is_admin, 
                    created_at, updated_at, published_at
                )
                VALUES ($1, FALSE, NOW(), NOW(), NOW())
                RETURNING id
            ''', telegram_id_str)
        
        # Проверяем не состоит ли уже
        exists = await conn.fetchval('''
            SELECT 1 FROM managers_organization_lnk 
            WHERE manager_id = $1 AND organization_id = $2
        ''', manager_id, org_id)
        
        if not exists:
            await conn.execute('''
                INSERT INTO managers_organization_lnk (manager_id, organization_id)
                VALUES ($1, $2)
            ''', manager_id, org_id)
        
        return org_id

# ============================================
# DATABASE FUNCTIONS - APARTMENTS
# ============================================

async def get_organization_apartments(org_id: int) -> List[Tuple[int, str, str, bool]]:
    """Получить список квартир организации"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT a.id, a.name, a.address, a.is_long
            FROM apartments a
            JOIN apartments_organization_lnk aol ON a.id = aol.apartment_id
            WHERE aol.organization_id = $1
            ORDER BY a.created_at DESC
        ''', org_id)
        
        return [(row['id'], row['name'], row['address'] or '', row['is_long']) for row in rows]

async def create_apartment(org_id: int, name: str, address: str) -> int:
    """Создать новую квартиру"""
    async with db_pool.acquire() as conn:
        # Создаём квартиру
        apt_id = await conn.fetchval('''
            INSERT INTO apartments (
                name, address, is_long, is_hidden,
                created_at, updated_at, published_at
            )
            VALUES ($1, $2, FALSE, FALSE, NOW(), NOW(), NOW())
            RETURNING id
        ''', name, address)
        
        # Связываем с организацией
        await conn.execute('''
            INSERT INTO apartments_organization_lnk (apartment_id, organization_id)
            VALUES ($1, $2)
        ''', apt_id, org_id)
        
        logger.info(f"✅ Created apartment {apt_id} for organization {org_id}")
        return apt_id

async def get_apartment_info(apt_id: int) -> Optional[Dict]:
    """Получить информацию о квартире"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT a.id, a.name, a.address, a.is_long,
                   aol.organization_id
            FROM apartments a
            LEFT JOIN apartments_organization_lnk aol ON a.id = aol.apartment_id
            WHERE a.id = $1
        ''', apt_id)
        
        return dict(row) if row else None

async def toggle_apartment_term(apt_id: int):
    """Переключить долгосрок/краткосрок"""
    async with db_pool.acquire() as conn:
        await conn.execute('''
            UPDATE apartments 
            SET is_long = NOT is_long, updated_at = NOW()
            WHERE id = $1
        ''', apt_id)

async def delete_apartment(apt_id: int):
    """Удалить квартиру"""
    async with db_pool.acquire() as conn:
        # Сначала удаляем связи
        await conn.execute('DELETE FROM apartments_organization_lnk WHERE apartment_id = $1', apt_id)
        await conn.execute('DELETE FROM infos_apartment_lnk WHERE apartment_id = $1', apt_id)
        await conn.execute('DELETE FROM bookings_apartment_lnk WHERE apartment_id = $1', apt_id)
        
        # Удаляем квартиру
        await conn.execute('DELETE FROM apartments WHERE id = $1', apt_id)
        
        logger.info(f"✅ Deleted apartment {apt_id}")

# ============================================
# DATABASE FUNCTIONS - INFOS & CATEGORIES
# ============================================

async def get_or_create_category(name: str, parent_name: str = None) -> int:
    """Получить или создать категорию"""
    async with db_pool.acquire() as conn:
        # Проверяем существует ли
        cat_id = await conn.fetchval(
            'SELECT id FROM categories WHERE name = $1',
            name
        )
        
        if cat_id:
            return cat_id
        
        # Создаём новую
        cat_id = await conn.fetchval('''
            INSERT INTO categories (
                name, expandable, editable,
                created_at, updated_at, published_at
            )
            VALUES ($1, TRUE, TRUE, NOW(), NOW(), NOW())
            RETURNING id
        ''', name)
        
        # Если указана родительская категория - создаём связь
        if parent_name:
            parent_id = await conn.fetchval(
                'SELECT id FROM categories WHERE name = $1',
                parent_name
            )
            
            if parent_id:
                await conn.execute('''
                    INSERT INTO categories_parent_lnk (category_id, inv_category_id)
                    VALUES ($1, $2)
                ''', cat_id, parent_id)
        
        logger.info(f"✅ Created category: {name}")
        return cat_id

async def save_apartment_field(
    apt_id: int, 
    section: str, 
    field_key: str,
    field_name: str,
    text_content: str = None,
    file_id: str = None,
    file_type: str = None
):
    """Сохранить информацию о квартире"""
    async with db_pool.acquire() as conn:
        # Получаем или создаём категории
        section_name = SECTION_TO_CATEGORY_MAP.get(section, section)
        field_category_name = FIELD_TO_CATEGORY_MAP.get(field_key, field_name)
        
        section_cat_id = await get_or_create_category(section_name)
        field_cat_id = await get_or_create_category(field_category_name, section_name)
        
        # Проверяем существует ли info
        info_id = await conn.fetchval('''
            SELECT i.id FROM infos i
            JOIN infos_apartment_lnk ial ON i.id = ial.info_id
            JOIN infos_category_lnk icl ON i.id = icl.info_id
            WHERE ial.apartment_id = $1 
            AND icl.category_id = $2
            LIMIT 1
        ''', apt_id, field_cat_id)
        
        if info_id:
            # Обновляем существующий
            await conn.execute('''
                UPDATE infos 
                SET name = $1, text = $2, type = $3, updated_at = NOW()
                WHERE id = $4
            ''', field_name, text_content, file_type or 'text', info_id)
        else:
            # Создаём новый
            info_id = await conn.fetchval('''
                INSERT INTO infos (
                    name, text, type, caption,
                    created_at, updated_at, published_at
                )
                VALUES ($1, $2, $3, $4, NOW(), NOW(), NOW())
                RETURNING id
            ''', field_name, text_content, file_type or 'text', file_id)
            
            # Связываем с квартирой
            await conn.execute('''
                INSERT INTO infos_apartment_lnk (info_id, apartment_id)
                VALUES ($1, $2)
            ''', info_id, apt_id)
            
            # Связываем с категорией
            await conn.execute('''
                INSERT INTO infos_category_lnk (info_id, category_id)
                VALUES ($1, $2)
            ''', info_id, field_cat_id)

async def get_apartment_field(apt_id: int, section: str, field_key: str) -> Optional[Dict]:
    """Получить информацию о поле квартиры"""
    field_category_name = FIELD_TO_CATEGORY_MAP.get(field_key, field_key)
    
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT i.name, i.text, i.type, i.caption
            FROM infos i
            JOIN infos_apartment_lnk ial ON i.id = ial.info_id
            JOIN infos_category_lnk icl ON i.id = icl.info_id
            JOIN categories c ON icl.category_id = c.id
            WHERE ial.apartment_id = $1 AND c.name = $2
            LIMIT 1
        ''', apt_id, field_category_name)
        
        if not row:
            return None
        
        return {
            'text_content': row['text'],
            'file_id': row['caption'],  # Используем caption для file_id
            'file_type': row['type']
        }

async def get_section_fields(apt_id: int, section: str) -> List[Dict]:
    """Получить все поля раздела"""
    section_name = SECTION_TO_CATEGORY_MAP.get(section, section)
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT i.name as field_name, i.text, i.type, i.caption,
                   c.name as category_name
            FROM infos i
            JOIN infos_apartment_lnk ial ON i.id = ial.info_id
            JOIN infos_category_lnk icl ON i.id = icl.info_id
            JOIN categories c ON icl.category_id = c.id
            JOIN categories_parent_lnk cpl ON c.id = cpl.category_id
            JOIN categories parent ON cpl.inv_category_id = parent.id
            WHERE ial.apartment_id = $1 AND parent.name = $2
            ORDER BY i.created_at
        ''', apt_id, section_name)
        
        result = []
        for row in rows:
            result.append({
                'field_key': row['category_name'].lower().replace(' ', '_'),
                'field_name': row['field_name'],
                'text_content': row['text'],
                'file_id': row['caption'],
                'file_type': row['type']
            })
        
        return result

async def get_filled_fields(apt_id: int, section: str) -> set:
    """Получить список заполненных полей раздела"""
    fields = await get_section_fields(apt_id, section)
    return set(f['field_key'] for f in fields if f['text_content'] or f['file_id'])

# ============================================
# DATABASE FUNCTIONS - BOOKINGS
# ============================================

async def create_booking(apt_id: int, guest_name: str, checkin_date) -> Tuple[int, str]:
    """Создать бронирование"""
    hash_code = generate_hash()
    
    if isinstance(checkin_date, str):
        checkin_date = datetime.strptime(checkin_date, '%Y-%m-%d').date()
    
    async with db_pool.acquire() as conn:
        # Создаём бронирование
        booking_id = await conn.fetchval('''
            INSERT INTO bookings (
                hash, guest_name, checkin, is_used, is_complete, current_status,
                created_at, updated_at, published_at
            )
            VALUES ($1, $2, $3, FALSE, FALSE, 'active', NOW(), NOW(), NOW())
            RETURNING id
        ''', hash_code, guest_name, checkin_date)
        
        # Связываем с квартирой
        await conn.execute('''
            INSERT INTO bookings_apartment_lnk (booking_id, apartment_id)
            VALUES ($1, $2)
        ''', booking_id, apt_id)
        
        logger.info(f"✅ Created booking {booking_id} for apartment {apt_id}")
        return booking_id, hash_code

async def get_apartment_bookings(apt_id: int) -> List[Dict]:
    """Получить бронирования квартиры"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT b.id, b.guest_name, b.checkin, b.checkout, b.hash,
                   b.is_complete, b.current_status
            FROM bookings b
            JOIN bookings_apartment_lnk bal ON b.id = bal.booking_id
            WHERE bal.apartment_id = $1 AND b.is_complete = FALSE
            ORDER BY b.checkin DESC
        ''', apt_id)
        
        return [dict(row) for row in rows]

async def get_booking_by_hash(hash_code: str) -> Optional[Dict]:
    """Получить бронирование по hash"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT b.id, b.guest_name, b.checkin, b.is_complete,
                   a.id as apartment_id, a.name as apartment_name, a.address
            FROM bookings b
            JOIN bookings_apartment_lnk bal ON b.id = bal.booking_id
            JOIN apartments a ON bal.apartment_id = a.id
            WHERE b.hash = $1
        ''', hash_code)
        
        return dict(row) if row else None

async def complete_booking(booking_id: int):
    """Завершить бронирование"""
    async with db_pool.acquire() as conn:
        await conn.execute('''
            UPDATE bookings 
            SET is_complete = TRUE, is_used = TRUE, 
                current_status = 'completed', updated_at = NOW()
            WHERE id = $1
        ''', booking_id)

# ============================================
# DATABASE FUNCTIONS - MANAGERS
# ============================================

async def get_organization_managers(org_id: int) -> List[Dict]:
    """Получить менеджеров организации"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT m.id, m.telegram_id, m.name, m.lastname, m.is_admin, m.is_owner
            FROM managers m
            JOIN managers_organization_lnk mol ON m.id = mol.manager_id
            WHERE mol.organization_id = $1
            ORDER BY m.is_owner DESC, m.is_admin DESC, m.name
        ''', org_id)
        
        result = []
        for row in rows:
            result.append({
                'telegram_id': row['telegram_id'],
                'username': row['lastname'] or '',  # lastname используем как username
                'first_name': row['name'] or 'Менеджер',
                'is_admin': row['is_admin'] or row['is_owner']
            })
        
        return result

# ============================================
# KEYBOARD FUNCTIONS
# ============================================

def get_main_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Добавление и настройка объектов", callback_data="objects_menu")],
        [InlineKeyboardButton(text="🏢 Личный кабинет компании", callback_data="organization_cabinet")],
        [InlineKeyboardButton(text="💡 Что улучшить в боте", callback_data="suggest_improvement")]
    ])

def get_add_organization_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Добавить компанию", callback_data="add_organization")]
    ])

def get_back_keyboard(callback="back"):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=callback)]
    ])

def get_home_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Главное меню", callback_data="home_main_menu")],
        [InlineKeyboardButton(text="Полезные функции 🔥", callback_data="home_useful_sections")]
    ])

def get_useful_sections_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Найти новую квартиру 🏠", url=USEFUL_LINKS['new_apartment'])],
        [InlineKeyboardButton(text="Задать вопрос бухгалтеру 💵", url=USEFUL_LINKS['accountant'])],
        [InlineKeyboardButton(text="Сделать общую закупку 📦", url=USEFUL_LINKS['group_purchase'])],
        [InlineKeyboardButton(text="Обменяться гостями 👥", url=USEFUL_LINKS['guest_exchange'])],
        [InlineKeyboardButton(text="Задать вопрос юристу 📄", url=USEFUL_LINKS['lawyer'])],
        [InlineKeyboardButton(text="Найти выгодную локацию 📍", url=USEFUL_LINKS['location'])],
        [InlineKeyboardButton(text="Обустроить квартиру 🪑", url=USEFUL_LINKS['furnish'])],
        [InlineKeyboardButton(text="Инвестировать 📊", url=USEFUL_LINKS['invest'])],
        [InlineKeyboardButton(text="Полезные книги 📚", url=USEFUL_LINKS['books'])],
        [InlineKeyboardButton(text="Психология 🧠", url=USEFUL_LINKS['psychology'])],
        [InlineKeyboardButton(text="Стать участником 🔥", url=USEFUL_LINKS['join_chat'])],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_home")]
    ])

def get_organization_cabinet_keyboard(org_info: Dict):
    long_term_text = "Да" if org_info.get('is_long') else "Нет"
    timezone_text = org_info.get('timezone', 'UTC+3')
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Изменить название", callback_data="edit_org_name")],
        [InlineKeyboardButton(text="Изменить город", callback_data="edit_org_city")],
        [InlineKeyboardButton(text="Изменить приветствие", callback_data="edit_org_greeting")],
        [InlineKeyboardButton(text=f"Часовой пояс: {timezone_text}", callback_data="edit_org_timezone")],
        [InlineKeyboardButton(text=f"Время заезда {org_info.get('check_in', '14:00')}", callback_data="edit_checkin_time")],
        [InlineKeyboardButton(text=f"Только долгосрок: {long_term_text}", callback_data="toggle_long_term")],
        [InlineKeyboardButton(text=f"Время выезда {org_info.get('check_out', '12:00')}", callback_data="edit_checkout_time")],
        [InlineKeyboardButton(text="Пригласить менеджера", callback_data="invite_manager")],
        [InlineKeyboardButton(text="Менеджеры", callback_data="managers_list")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])

def get_apartments_list_keyboard(apartments: List[Tuple]):
    buttons = []
    for apt_id, name, address, is_long in apartments:
        buttons.append([InlineKeyboardButton(text=name, callback_data=f"apartment_{apt_id}")])
    
    buttons.append([InlineKeyboardButton(text="➕ Добавить объект", callback_data="add_apartment")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_apartment_menu_keyboard(apt_id: int, is_long: bool = False):
    term_button_text = "📅 Долгосрок" if is_long else "📅 Краткосрок"
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧳 Заселение", callback_data=f"section_checkin_{apt_id}")],
        [InlineKeyboardButton(text="📹 Аренда", callback_data=f"section_rent_{apt_id}")],
        [InlineKeyboardButton(text="🍿 Впечатления", callback_data=f"section_experiences_{apt_id}")],
        [InlineKeyboardButton(text="📦 Выселение", callback_data=f"section_checkout_{apt_id}")],
        [InlineKeyboardButton(text="🔗 Бронирования", callback_data=f"bookings_{apt_id}")],
        [InlineKeyboardButton(text=term_button_text, callback_data=f"toggle_term_{apt_id}")],
        [InlineKeyboardButton(text="Ссылка для собственника", callback_data=f"owner_link_{apt_id}")],
        [InlineKeyboardButton(text="Редактировать объект", callback_data=f"edit_apartment_{apt_id}")],
        [InlineKeyboardButton(text="Предпросмотр объекта", callback_data=f"apt_preview_{apt_id}")],
        [InlineKeyboardButton(text="Удалить объект", callback_data=f"delete_apartment_{apt_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="objects_menu")]
    ])

async def get_checkin_section_keyboard_async(apt_id: int, filled_fields: set = None):
    """Клавиатура раздела Заселение"""
    filled_fields = filled_fields or set()
    
    def field_text(name: str, key: str) -> str:
        return f"{name} ■" if key in filled_fields else name
    
    buttons = [
        [InlineKeyboardButton(text=field_text("🕐 Время заселения", "checkin_time"), callback_data=f"field_checkin_time_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🚗 Парковка", "parking"), callback_data=f"field_parking_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🌐 Wi-Fi", "wifi"), callback_data=f"field_wifi_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🔑 Ключ от двери", "door_key"), callback_data=f"field_door_key_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🗺 Как найти объект?", "how_to_find"), callback_data=f"field_how_to_find_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🚶 Как дойти", "how_to_reach"), callback_data=f"field_how_to_reach_{apt_id}")],
        [InlineKeyboardButton(text=field_text("📄 Документы", "documents"), callback_data=f"field_documents_{apt_id}")],
        [InlineKeyboardButton(text=field_text("💰 Депозит", "deposit"), callback_data=f"field_deposit_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🔐 Дист. заселение", "remote_checkin"), callback_data=f"field_remote_checkin_{apt_id}")],
        [InlineKeyboardButton(text="🏠 Помощь с проживанием", callback_data=f"subsection_help_{apt_id}")],
        [InlineKeyboardButton(text="📍 Магазины, аптеки", callback_data=f"subsection_stores_{apt_id}")],
        [InlineKeyboardButton(text=field_text("📢 Правила", "rules"), callback_data=f"field_rules_{apt_id}")],
    ]
    
    buttons.append([InlineKeyboardButton(text="➕ Добавить кнопку", callback_data=f"add_custom_checkin_{apt_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"apartment_{apt_id}")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_rent_section_keyboard(apt_id: int, filled_fields: set = None):
    filled_fields = filled_fields or set()
    
    def field_text(name: str, key: str) -> str:
        return f"{name} ■" if key in filled_fields else name
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=field_text("📱 Телефоны УК", "uk_phones"), callback_data=f"field_uk_phones_{apt_id}")],
        [InlineKeyboardButton(text=field_text("👨‍💼 Диспетчер", "dispatcher"), callback_data=f"field_dispatcher_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🆘 Аварийка", "emergency"), callback_data=f"field_emergency_{apt_id}")],
        [InlineKeyboardButton(text=field_text("💬 Чаты", "chats"), callback_data=f"field_chats_{apt_id}")],
        [InlineKeyboardButton(text=field_text("📝 Обратная связь", "feedback_form"), callback_data=f"field_feedback_form_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🌐 Интернет", "internet"), callback_data=f"field_internet_{apt_id}")],
        [InlineKeyboardButton(text="➕ Добавить кнопку", callback_data=f"add_custom_rent_{apt_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"apartment_{apt_id}")]
    ])

def get_help_subsection_keyboard(apt_id: int, filled_fields: set = None):
    filled_fields = filled_fields or set()
    
    def field_text(name: str, key: str) -> str:
        return f"{name} ■" if key in filled_fields else name
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=field_text("🥐 Завтрак", "breakfast"), callback_data=f"field_breakfast_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🛏 Бельё", "linen"), callback_data=f"field_linen_{apt_id}")],
        [InlineKeyboardButton(text=field_text("📱 Менеджер", "manager_contact"), callback_data=f"field_manager_contact_{apt_id}")],
        [InlineKeyboardButton(text=field_text("📺 ТВ", "tv_setup"), callback_data=f"field_tv_setup_{apt_id}")],
        [InlineKeyboardButton(text=field_text("❄️ Кондиционер", "ac"), callback_data=f"field_ac_{apt_id}")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data=f"add_custom_help_{apt_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_checkin_{apt_id}")]
    ])

def get_stores_subsection_keyboard(apt_id: int, filled_fields: set = None):
    filled_fields = filled_fields or set()
    
    def field_text(name: str, key: str) -> str:
        return f"{name} ■" if key in filled_fields else name
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=field_text("🛒 Магазины", "shops"), callback_data=f"field_shops_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🚗 Аренда авто", "car_rental"), callback_data=f"field_car_rental_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🏃 Спорт", "sport"), callback_data=f"field_sport_{apt_id}")],
        [InlineKeyboardButton(text=field_text("💊 Больницы", "hospitals"), callback_data=f"field_hospitals_{apt_id}")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data=f"add_custom_stores_{apt_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_checkin_{apt_id}")]
    ])

def get_experiences_section_keyboard(apt_id: int, filled_fields: set = None):
    filled_fields = filled_fields or set()
    
    def field_text(name: str, key: str) -> str:
        return f"{name} ■" if key in filled_fields else name
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=field_text("🗿 Экскурсии", "excursions"), callback_data=f"field_excursions_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🏛 Музеи", "museums"), callback_data=f"field_museums_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🌳 Парки", "parks"), callback_data=f"field_parks_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🎬 Развлечения", "entertainment"), callback_data=f"field_entertainment_{apt_id}")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data=f"add_custom_exp_{apt_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"apartment_{apt_id}")]
    ])

def get_checkout_section_keyboard(apt_id: int, filled_fields: set = None):
    filled_fields = filled_fields or set()
    
    def field_text(name: str, key: str) -> str:
        return f"{name} ■" if key in filled_fields else name
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=field_text("🚪 Выезд без менеджера", "self_checkout"), callback_data=f"field_self_checkout_{apt_id}")],
        [InlineKeyboardButton(text=field_text("💸 Возврат депозита", "deposit_return"), callback_data=f"field_deposit_return_{apt_id}")],
        [InlineKeyboardButton(text=field_text("📅 Продление", "extend_stay"), callback_data=f"field_extend_stay_{apt_id}")],
        [InlineKeyboardButton(text=field_text("🎁 Скидки", "discounts"), callback_data=f"field_discounts_{apt_id}")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data=f"add_custom_checkout_{apt_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"apartment_{apt_id}")]
    ])

def get_field_edit_keyboard(apt_id: int, section: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_{section}_{apt_id}")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"skip_field_{section}_{apt_id}")]
    ])

# ============================================
# КОМАНДЫ
# ============================================

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    """Команда /start"""
    telegram_id = message.from_user.id
    telegram_id_str = telegram_id_to_str(telegram_id)
    
    # Проверяем параметры
    start_param = message.text.split()[1] if len(message.text.split()) > 1 else None
    
    # Режим гостя
    if start_param and start_param.startswith("guest_"):
        hash_code = start_param.replace("guest_", "")
        booking = await get_booking_by_hash(hash_code)
        
        if booking and not booking['is_complete']:
            apt_id = booking['apartment_id']
            apt_name = booking['apartment_name']
            address = booking['address'] or "Москва"
            
            text = f"{apt_name}\n\nАдрес: {address}.\n\nИнформация для изучения:"
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➡️ Начать", callback_data=f"guest_start_{apt_id}")],
                [InlineKeyboardButton(text="🚕 Такси", url="https://taxi.yandex.ru")]
            ])
            
            await message.answer(text, reply_markup=keyboard)
            return
        else:
            await message.answer("Бронирование не найдено или завершено.")
            return
    
    # Присоединение по hash
    if start_param and start_param.startswith("org_"):
        hash_code = start_param.replace("org_", "")
        org_id = await join_organization_by_hash(telegram_id, hash_code)
        
        if org_id:
            await state.update_data(current_organization_id=org_id)
            org_info = await get_organization_info(org_id)
            await message.answer(f"✅ Присоединились к «{org_info['name']}»!")
            
            await message.answer(
                "Главное меню бота 🏠",
                reply_markup=get_main_menu_keyboard()
            )
            return
        else:
            await message.answer("Неверная ссылка приглашения.")
            return
    
    # Режим менеджера
    organizations = await get_manager_organizations(telegram_id)
    
    if not organizations:
        await message.answer(
            "Создайте компанию или присоединитесь по ссылке.",
            reply_markup=get_add_organization_keyboard()
        )
    else:
        await state.update_data(current_organization_id=organizations[0][0])
        await message.answer(
            "Главное меню бота 🏠\n\nНастройте объекты или компанию.",
            reply_markup=get_main_menu_keyboard()
        )

@dp.message(Command("menu"))
async def cmd_menu(message: types.Message, state: FSMContext):
    """Команда /menu"""
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    if not org_id:
        organizations = await get_manager_organizations(message.from_user.id)
        if organizations:
            org_id = organizations[0][0]
            await state.update_data(current_organization_id=org_id)
    
    await message.answer(
        "Главное меню",
        reply_markup=get_main_menu_keyboard()
    )

@dp.message(Command("home"))
async def cmd_home(message: types.Message):
    """Команда /home"""
    await message.answer(
        "Вы в боте 🤖",
        reply_markup=get_home_keyboard()
    )

# ============================================
# ОСНОВНЫЕ ОБРАБОТЧИКИ
# ============================================

@dp.callback_query(F.data == "main_menu")
async def main_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Главное меню бота 🏠",
        reply_markup=get_main_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "home_main_menu")
async def home_main_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    if not org_id:
        organizations = await get_manager_organizations(callback.from_user.id)
        if organizations:
            org_id = organizations[0][0]
            await state.update_data(current_organization_id=org_id)
    
    await callback.message.edit_text(
        "Главное меню бота 🏠",
        reply_markup=get_main_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "home_useful_sections")
async def home_useful_sections_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Полезные разделы",
        reply_markup=get_useful_sections_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_home")
async def back_to_home_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "Вы в боте 🤖",
        reply_markup=get_home_keyboard()
    )
    await callback.answer()

# ============================================
# СОЗДАНИЕ ОРГАНИЗАЦИИ
# ============================================

@dp.callback_query(F.data == "add_organization")
async def add_organization(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Напишите название компании:",
        reply_markup=get_back_keyboard("start")
    )
    await state.set_state(OrganizationStates.waiting_name)
    await callback.answer()

@dp.message(OrganizationStates.waiting_name)
async def process_organization_name(message: types.Message, state: FSMContext):
    await state.update_data(organization_name=message.text)
    await message.answer(
        "Напишите город компании:",
        reply_markup=get_back_keyboard("cancel")
    )
    await state.set_state(OrganizationStates.waiting_city)

@dp.message(OrganizationStates.waiting_city)
async def process_organization_city(message: types.Message, state: FSMContext):
    data = await state.get_data()
    org_name = data.get('organization_name')
    
    if not org_name:
        await state.clear()
        await message.answer(
            "❌ Ошибка. Начните заново.",
            reply_markup=get_back_keyboard("start")
        )
        return
    
    org_city = message.text
    org_id = await create_organization(org_name, org_city, message.from_user.id)
    
    await state.update_data(current_organization_id=org_id)
    await message.answer(
        f"Отлично! Компания создана.\n\nНазвание: {org_name}\nГород: {org_city}",
        reply_markup=get_main_menu_keyboard()
    )
    
    await state.set_data({'current_organization_id': org_id})

@dp.callback_query(F.data == "cancel")
async def cancel_creation(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    
    organizations = await get_manager_organizations(callback.from_user.id)
    
    if organizations:
        await state.update_data(current_organization_id=organizations[0][0])
        await callback.message.edit_text(
            "Главное меню",
            reply_markup=get_main_menu_keyboard()
        )
    else:
        await callback.message.edit_text(
            "Создайте компанию",
            reply_markup=get_add_organization_keyboard()
        )
    
    await callback.answer("Отменено")

# ============================================
# ЛИЧНЫЙ КАБИНЕТ ОРГАНИЗАЦИИ  
# ============================================

@dp.callback_query(F.data == "organization_cabinet")
async def organization_cabinet(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    if not org_id:
        organizations = await get_manager_organizations(callback.from_user.id)
        if organizations:
            org_id = organizations[0][0]
            await state.update_data(current_organization_id=org_id)
        else:
            await callback.message.edit_text(
                "Создайте компанию",
                reply_markup=get_add_organization_keyboard()
            )
            await callback.answer("⚠️ Создайте компанию", show_alert=True)
            return
    
    org_info = await get_organization_info(org_id)
    
    if org_info:
        text = (
            f"{org_info['name']}\n"
            f"{org_info['city']}\n\n"
            f"Приветствие:\n{org_info.get('greeting', '')}"
        )
        await callback.message.edit_text(text, reply_markup=get_organization_cabinet_keyboard(org_info))
    
    await callback.answer()

@dp.callback_query(F.data == "invite_manager")
async def invite_manager(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    org_info = await get_organization_info(org_id)
    bot_username = (await bot.get_me()).username
    invite_link = f"https://t.me/{bot_username}?start=org_{org_info['hash']}"
    
    text = f"Ссылка для приглашения:\n\n{invite_link}"
    
    await callback.message.answer(text)
    await callback.answer()

@dp.callback_query(F.data == "managers_list")
async def managers_list(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    if not org_id:
        await callback.answer("Ошибка", show_alert=True)
        return
    
    managers = await get_organization_managers(org_id)
    
    text = "Менеджеры компании:\n\n"
    
    if managers:
        for manager in managers:
            username = manager['username'] or "Без username"
            first_name = manager['first_name']
            role = "👑 Админ" if manager['is_admin'] else "👤 Менеджер"
            text += f"• {role} - {first_name} (@{username})\n"
    else:
        text += "Нет менеджеров"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пригласить", callback_data="invite_manager")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="organization_cabinet")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# ============================================
# МЕНЮ ОБЪЕКТОВ
# ============================================

@dp.callback_query(F.data == "objects_menu")
async def objects_menu(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    if not org_id:
        organizations = await get_manager_organizations(callback.from_user.id)
        if organizations:
            org_id = organizations[0][0]
            await state.update_data(current_organization_id=org_id)
        else:
            await callback.message.edit_text(
                "Создайте компанию",
                reply_markup=get_add_organization_keyboard()
            )
            await callback.answer("⚠️ Создайте компанию", show_alert=True)
            return
    
    apartments = await get_organization_apartments(org_id)
    await callback.message.edit_text(
        "Список ваших объектов:",
        reply_markup=get_apartments_list_keyboard(apartments)
    )
    await callback.answer()

# ============================================
# ДОБАВЛЕНИЕ КВАРТИРЫ
# ============================================

@dp.callback_query(F.data == "add_apartment")
async def add_apartment(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Введите название объекта:",
        reply_markup=get_back_keyboard("objects_menu")
    )
    await state.set_state(ApartmentStates.waiting_name)
    await callback.answer()

@dp.message(ApartmentStates.waiting_name)
async def process_apartment_name(message: types.Message, state: FSMContext):
    await state.update_data(apartment_name=message.text)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="objects_menu")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip_address")]
    ])
    
    await message.answer("Введите адрес объекта:", reply_markup=keyboard)
    await state.set_state(ApartmentStates.waiting_address)

@dp.message(ApartmentStates.waiting_address)
async def process_apartment_address(message: types.Message, state: FSMContext):
    data = await state.get_data()
    apt_name = data.get('apartment_name')
    org_id = data.get('current_organization_id')
    
    if not apt_name:
        await state.clear()
        await state.update_data(current_organization_id=org_id)
        await message.answer(
            "❌ Ошибка. Начните заново.",
            reply_markup=get_back_keyboard("objects_menu")
        )
        return
    
    apt_address = message.text
    apt_id = await create_apartment(org_id, apt_name, apt_address)
    
    await state.update_data(pending_apartment_id=apt_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 Сохранить", callback_data=f"confirm_save_{apt_id}")],
        [InlineKeyboardButton(text="❌ Не сохранять", callback_data="objects_menu")]
    ])
    
    await message.answer("Сохранить объект?", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("confirm_save_"))
async def confirm_save(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    apartments = await get_organization_apartments(org_id)
    await clear_state_keep_company(state)
    
    await callback.message.edit_text(
        "Список ваших объектов:",
        reply_markup=get_apartments_list_keyboard(apartments)
    )
    await callback.answer("Объект сохранен!")

@dp.callback_query(F.data == "skip_address")
async def skip_address(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    apt_name = data.get('apartment_name')
    org_id = data.get('current_organization_id')
    
    if not apt_name:
        await state.clear()
        await state.update_data(current_organization_id=org_id)
        await callback.message.edit_text(
            "❌ Ошибка.",
            reply_markup=get_back_keyboard("objects_menu")
        )
        return
    
    apt_id = await create_apartment(org_id, apt_name, "")
    await state.update_data(pending_apartment_id=apt_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 Сохранить", callback_data=f"confirm_save_{apt_id}")],
        [InlineKeyboardButton(text="❌ Не сохранять", callback_data="objects_menu")]
    ])
    
    await callback.message.edit_text("Сохранить объект?", reply_markup=keyboard)
    await callback.answer()

# ============================================
# ПРОСМОТР КВАРТИРЫ
# ============================================

@dp.callback_query(F.data.startswith("apartment_") & ~F.data.startswith("apt_preview_"))
async def view_apartment(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[1])
    apt_info = await get_apartment_info(apt_id)
    
    if apt_info:
        apt_name = apt_info['name']
        is_long = apt_info['is_long']
        
        text = f"Объект: {apt_name}"
        await callback.message.edit_text(text, reply_markup=get_apartment_menu_keyboard(apt_id, is_long))
    
    await callback.answer()

@dp.callback_query(F.data.startswith("toggle_term_"))
async def toggle_term_handler(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[2])
    await toggle_apartment_term(apt_id)
    
    apt_info = await get_apartment_info(apt_id)
    apt_name = apt_info['name']
    is_long = apt_info['is_long']
    
    mode_text = "долгосрочная аренда" if is_long else "краткосрочная аренда"
    
    text = f"Объект: {apt_name}"
    await callback.message.edit_text(text, reply_markup=get_apartment_menu_keyboard(apt_id, is_long))
    await callback.answer(f"✅ {mode_text}")

@dp.callback_query(F.data.startswith("delete_apartment_"))
async def confirm_delete_apartment(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[2])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_delete_{apt_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"apartment_{apt_id}")]
    ])
    
    await callback.message.edit_text("Удалить объект?", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("confirm_delete_"))
async def delete_apartment_confirmed(callback: types.CallbackQuery, state: FSMContext):
    apt_id = int(callback.data.split("_")[2])
    await delete_apartment(apt_id)
    
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    apartments = await get_organization_apartments(org_id)
    
    await callback.message.edit_text(
        "Объект удален.",
        reply_markup=get_apartments_list_keyboard(apartments)
    )
    await callback.answer("Удалено")

# ============================================
# РАЗДЕЛЫ КВАРТИРЫ
# ============================================

@dp.callback_query(F.data.startswith("section_checkin_"))
async def section_checkin(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[2])
    
    filled_checkin = await get_filled_fields(apt_id, 'checkin')
    filled_help = await get_filled_fields(apt_id, 'help')
    filled_stores = await get_filled_fields(apt_id, 'stores')
    
    all_filled = filled_checkin | filled_help | filled_stores
    
    keyboard = await get_checkin_section_keyboard_async(apt_id, all_filled)
    
    await callback.message.edit_text(
        "Раздел 🧳 Заселение",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("section_rent_"))
async def section_rent(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[2])
    filled_fields = await get_filled_fields(apt_id, 'rent')
    
    await callback.message.edit_text(
        "Раздел 📹 Аренда",
        reply_markup=get_rent_section_keyboard(apt_id, filled_fields)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("subsection_help_"))
async def subsection_help(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[2])
    filled_fields = await get_filled_fields(apt_id, 'help')
    
    await callback.message.edit_text(
        "Подраздел 🏠 Помощь",
        reply_markup=get_help_subsection_keyboard(apt_id, filled_fields)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("subsection_stores_"))
async def subsection_stores(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[2])
    filled_fields = await get_filled_fields(apt_id, 'stores')
    
    await callback.message.edit_text(
        "Подраздел 📍 Магазины",
        reply_markup=get_stores_subsection_keyboard(apt_id, filled_fields)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("section_experiences_"))
async def section_experiences(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[2])
    filled_fields = await get_filled_fields(apt_id, 'experiences')
    
    await callback.message.edit_text(
        "Раздел 🍿 Впечатления",
        reply_markup=get_experiences_section_keyboard(apt_id, filled_fields)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("section_checkout_"))
async def section_checkout(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[2])
    filled_fields = await get_filled_fields(apt_id, 'checkout')
    
    await callback.message.edit_text(
        "Раздел 📦 Выселение",
        reply_markup=get_checkout_section_keyboard(apt_id, filled_fields)
    )
    await callback.answer()

# ============================================
# РЕДАКТИРОВАНИЕ ПОЛЕЙ
# ============================================

@dp.callback_query(F.data.startswith("field_"))
async def edit_field(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    
    parts = callback.data.split("_")
    field_key = "_".join(parts[1:-1])
    apt_id = int(parts[-1])
    
    field_name = FIELD_NAMES.get(field_key, "Поле")
    field_desc = FIELD_DESCRIPTIONS.get(field_key, "Введите содержимое:")
    
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
        editing_apartment_id=apt_id,
        editing_field_key=field_key,
        editing_field_name=field_name,
        editing_section=section
    )
    
    text = f"Редактируете кнопку\n\n{field_desc}"
    
    await callback.message.edit_text(text, reply_markup=get_field_edit_keyboard(apt_id, section))
    await state.set_state(ApartmentStates.editing_field)

@dp.message(ApartmentStates.editing_field)
async def process_field_content(message: types.Message, state: FSMContext):
    data = await state.get_data()
    apt_id = data.get('editing_apartment_id')
    field_key = data.get('editing_field_key')
    field_name = data.get('editing_field_name')
    section = data.get('editing_section')
    
    if not apt_id or not field_key or not section:
        await state.clear()
        await message.answer("❌ Ошибка", reply_markup=get_main_menu_keyboard())
        return
    
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
    
    await save_apartment_field(apt_id, section, field_key, field_name, text_content, file_id, file_type)
    
    filled_fields = await get_filled_fields(apt_id, section)
    
    if section == "help":
        keyboard = get_help_subsection_keyboard(apt_id, filled_fields)
        text = "Подраздел 🏠 Помощь"
    elif section == "stores":
        keyboard = get_stores_subsection_keyboard(apt_id, filled_fields)
        text = "Подраздел 📍 Магазины"
    elif section == "rent":
        keyboard = get_rent_section_keyboard(apt_id, filled_fields)
        text = "Раздел 📹 Аренда"
    elif section == "experiences":
        keyboard = get_experiences_section_keyboard(apt_id, filled_fields)
        text = "Раздел 🍿 Впечатления"
    elif section == "checkout":
        keyboard = get_checkout_section_keyboard(apt_id, filled_fields)
        text = "Раздел 📦 Выселение"
    else:
        filled_checkin = await get_filled_fields(apt_id, 'checkin')
        filled_help = await get_filled_fields(apt_id, 'help')
        filled_stores = await get_filled_fields(apt_id, 'stores')
        all_filled = filled_checkin | filled_help | filled_stores
        
        keyboard = await get_checkin_section_keyboard_async(apt_id, all_filled)
        text = "Раздел 🧳 Заселение"
    
    await message.answer(text, reply_markup=keyboard)
    await state.clear()

@dp.callback_query(F.data.startswith("skip_field_"))
async def skip_field(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    section = parts[2]
    apt_id = int(parts[3])
    
    if section == "help":
        filled_fields = await get_filled_fields(apt_id, 'help')
        keyboard = get_help_subsection_keyboard(apt_id, filled_fields)
        text = "Подраздел 🏠 Помощь"
    elif section == "stores":
        filled_fields = await get_filled_fields(apt_id, 'stores')
        keyboard = get_stores_subsection_keyboard(apt_id, filled_fields)
        text = "Подраздел 📍 Магазины"
    elif section == "rent":
        filled_fields = await get_filled_fields(apt_id, 'rent')
        keyboard = get_rent_section_keyboard(apt_id, filled_fields)
        text = "Раздел 📹 Аренда"
    elif section == "experiences":
        filled_fields = await get_filled_fields(apt_id, 'experiences')
        keyboard = get_experiences_section_keyboard(apt_id, filled_fields)
        text = "Раздел 🍿 Впечатления"
    elif section == "checkout":
        filled_fields = await get_filled_fields(apt_id, 'checkout')
        keyboard = get_checkout_section_keyboard(apt_id, filled_fields)
        text = "Раздел 📦 Выселение"
    else:
        filled_checkin = await get_filled_fields(apt_id, 'checkin')
        filled_help = await get_filled_fields(apt_id, 'help')
        filled_stores = await get_filled_fields(apt_id, 'stores')
        all_filled = filled_checkin | filled_help | filled_stores
        
        keyboard = await get_checkin_section_keyboard_async(apt_id, all_filled)
        text = "Раздел 🧳 Заселение"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await state.clear()
    await callback.answer()

# ============================================
# РЕДАКТИРОВАНИЕ ОРГАНИЗАЦИИ
# ============================================

@dp.callback_query(F.data == "edit_org_name")
async def edit_org_name(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Напишите название компании:",
        reply_markup=get_back_keyboard("organization_cabinet")
    )
    await state.set_state(OrganizationStates.editing_name)
    await callback.answer()

@dp.message(OrganizationStates.editing_name)
async def process_edit_org_name(message: types.Message, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    await update_organization_field(org_id, 'name', message.text)
    
    org_info = await get_organization_info(org_id)
    text = (
        f"{org_info['name']}\n"
        f"{org_info['city']}\n\n"
        f"Приветствие:\n{org_info.get('greeting', '')}"
    )
    await message.answer(text, reply_markup=get_organization_cabinet_keyboard(org_info))
    await state.set_data({'current_organization_id': org_id})

@dp.callback_query(F.data == "edit_org_city")
async def edit_org_city(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Напишите город компании:",
        reply_markup=get_back_keyboard("organization_cabinet")
    )
    await state.set_state(OrganizationStates.editing_city)
    await callback.answer()

@dp.message(OrganizationStates.editing_city)
async def process_edit_org_city(message: types.Message, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    await update_organization_field(org_id, 'city', message.text)
    
    org_info = await get_organization_info(org_id)
    text = (
        f"{org_info['name']}\n"
        f"{org_info['city']}\n\n"
        f"Приветствие:\n{org_info.get('greeting', '')}"
    )
    await message.answer(text, reply_markup=get_organization_cabinet_keyboard(org_info))
    await clear_state_keep_company(state)

@dp.callback_query(F.data == "edit_org_greeting")
async def edit_org_greeting(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Введите приветствие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="organization_cabinet")],
            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="organization_cabinet")]
        ])
    )
    await state.set_state(OrganizationStates.editing_greeting)
    await callback.answer()

@dp.message(OrganizationStates.editing_greeting)
async def process_edit_org_greeting(message: types.Message, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    await update_organization_field(org_id, 'greeting', message.text)
    
    org_info = await get_organization_info(org_id)
    text = (
        f"{org_info['name']}\n"
        f"{org_info['city']}\n\n"
        f"Приветствие:\n{org_info.get('greeting', '')}"
    )
    await message.answer(text, reply_markup=get_organization_cabinet_keyboard(org_info))
    await clear_state_keep_company(state)

@dp.callback_query(F.data == "edit_org_timezone")
async def edit_org_timezone(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Введите часовой пояс.\n\nПримеры:\nUTC+3 для Москвы\nUTC+5 для Екатеринбурга\nUTC+7 для Новосибирска",
        reply_markup=get_back_keyboard("organization_cabinet")
    )
    await state.set_state(OrganizationStates.waiting_timezone)
    await callback.answer()

@dp.message(OrganizationStates.waiting_timezone)
async def process_edit_timezone(message: types.Message, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    await update_organization_field(org_id, 'timezone', message.text)
    
    org_info = await get_organization_info(org_id)
    text = (
        f"{org_info['name']}\n"
        f"{org_info['city']}\n\n"
        f"Приветствие:\n{org_info.get('greeting', '')}"
    )
    await message.answer(text, reply_markup=get_organization_cabinet_keyboard(org_info))
    await clear_state_keep_company(state)

@dp.callback_query(F.data == "edit_checkin_time")
async def edit_checkin_time(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Введите время заезда в формате 12:00:",
        reply_markup=get_back_keyboard("organization_cabinet")
    )
    await state.set_state(OrganizationStates.waiting_checkin_time)
    await callback.answer()

@dp.message(OrganizationStates.waiting_checkin_time)
async def process_edit_checkin_time(message: types.Message, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    await update_organization_field(org_id, 'check_in', message.text)
    
    org_info = await get_organization_info(org_id)
    text = (
        f"{org_info['name']}\n"
        f"{org_info['city']}\n\n"
        f"Приветствие:\n{org_info.get('greeting', '')}"
    )
    await message.answer(text, reply_markup=get_organization_cabinet_keyboard(org_info))
    await clear_state_keep_company(state)

@dp.callback_query(F.data == "edit_checkout_time")
async def edit_checkout_time(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Введите время выезда в формате 12:00:",
        reply_markup=get_back_keyboard("organization_cabinet")
    )
    await state.set_state(OrganizationStates.waiting_checkout_time)
    await callback.answer()

@dp.message(OrganizationStates.waiting_checkout_time)
async def process_edit_checkout_time(message: types.Message, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    await update_organization_field(org_id, 'check_out', message.text)
    
    org_info = await get_organization_info(org_id)
    text = (
        f"{org_info['name']}\n"
        f"{org_info['city']}\n\n"
        f"Приветствие:\n{org_info.get('greeting', '')}"
    )
    await message.answer(text, reply_markup=get_organization_cabinet_keyboard(org_info))
    await clear_state_keep_company(state)

@dp.callback_query(F.data == "toggle_long_term")
async def toggle_long_term(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    async with db_pool.acquire() as conn:
        await conn.execute('''
            UPDATE organizations 
            SET is_long = NOT is_long, updated_at = NOW()
            WHERE id = $1
        ''', org_id)
    
    org_info = await get_organization_info(org_id)
    text = (
        f"{org_info['name']}\n"
        f"{org_info['city']}\n\n"
        f"Приветствие:\n{org_info.get('greeting', '')}"
    )
    await callback.message.edit_text(text, reply_markup=get_organization_cabinet_keyboard(org_info))
    await callback.answer()

# ============================================
# РЕДАКТИРОВАНИЕ ОБЪЕКТОВ
# ============================================

@dp.callback_query(F.data.startswith("edit_apartment_"))
async def edit_apartment_info(callback: types.CallbackQuery, state: FSMContext):
    apt_id = int(callback.data.split("_")[2])
    
    apt_info = await get_apartment_info(apt_id)
    
    if not apt_info:
        await callback.answer("Объект не найден", show_alert=True)
        return
    
    apt_name = apt_info['name']
    apt_address = apt_info['address'] or "Не указан"
    
    text = f"Редактирование объекта\n\n📝 Название: {apt_name}\n📍 Адрес: {apt_address}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Изменить название", callback_data=f"edit_apt_name_{apt_id}")],
        [InlineKeyboardButton(text="Изменить адрес", callback_data=f"edit_apt_addr_{apt_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"apartment_{apt_id}")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_apt_name_"))
async def edit_apartment_name_start(callback: types.CallbackQuery, state: FSMContext):
    apt_id = int(callback.data.split("_")[3])
    
    await state.update_data(editing_apartment_id=apt_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"edit_apartment_{apt_id}")],
        [InlineKeyboardButton(text="Пропустить", callback_data=f"edit_apartment_{apt_id}")]
    ])
    
    await callback.message.edit_text(
        "Введите название объекта:",
        reply_markup=keyboard
    )
    await state.set_state(ApartmentStates.editing_name)
    await callback.answer()

@dp.message(ApartmentStates.editing_name)
async def process_edit_apartment_name(message: types.Message, state: FSMContext):
    data = await state.get_data()
    apt_id = data['editing_apartment_id']
    new_name = message.text
    
    await state.update_data(new_apartment_name=new_name)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"edit_apartment_{apt_id}")],
        [InlineKeyboardButton(text="Сохранить", callback_data=f"confirm_apt_edit_{apt_id}")],
        [InlineKeyboardButton(text="Не сохранять", callback_data=f"edit_apartment_{apt_id}")]
    ])
    
    await message.answer("Сохранить объект?", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("edit_apt_addr_"))
async def edit_apartment_address_start(callback: types.CallbackQuery, state: FSMContext):
    apt_id = int(callback.data.split("_")[3])
    
    await state.update_data(editing_apartment_id=apt_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"edit_apartment_{apt_id}")],
        [InlineKeyboardButton(text="Пропустить", callback_data=f"edit_apartment_{apt_id}")]
    ])
    
    await callback.message.edit_text(
        "Введите адрес объекта:",
        reply_markup=keyboard
    )
    await state.set_state(ApartmentStates.editing_address)
    await callback.answer()

@dp.message(ApartmentStates.editing_address)
async def process_edit_apartment_address(message: types.Message, state: FSMContext):
    data = await state.get_data()
    apt_id = data['editing_apartment_id']
    new_address = message.text
    
    await state.update_data(new_apartment_address=new_address)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"edit_apartment_{apt_id}")],
        [InlineKeyboardButton(text="Сохранить", callback_data=f"confirm_apt_edit_{apt_id}")],
        [InlineKeyboardButton(text="Не сохранять", callback_data=f"edit_apartment_{apt_id}")]
    ])
    
    await message.answer("Сохранить объект?", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("confirm_apt_edit_"))
async def confirm_apartment_edit(callback: types.CallbackQuery, state: FSMContext):
    apt_id = int(callback.data.split("_")[3])
    data = await state.get_data()
    
    # Обновляем название если было изменено
    new_name = data.get('new_apartment_name')
    if new_name:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                UPDATE apartments 
                SET name = $1, updated_at = NOW()
                WHERE id = $2
            ''', new_name, apt_id)
    
    # Обновляем адрес если был изменен
    new_address = data.get('new_apartment_address')
    if new_address:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                UPDATE apartments 
                SET address = $1, updated_at = NOW()
                WHERE id = $2
            ''', new_address, apt_id)
    
    await clear_state_keep_company(state)
    
    apt_info = await get_apartment_info(apt_id)
    apt_name = apt_info['name']
    is_long = apt_info['is_long']
    
    text = f"Объект: {apt_name}"
    await callback.message.edit_text(text, reply_markup=get_apartment_menu_keyboard(apt_id, is_long))
    await callback.answer("✅ Изменения сохранены!")

# ============================================
# КАСТОМНЫЕ КНОПКИ
# ============================================

async def get_custom_fields(apt_id: int, section: str) -> List[Dict]:
    """Получить кастомные поля раздела"""
    section_name = SECTION_TO_CATEGORY_MAP.get(section, section)
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT i.id, i.name as field_name, i.text, i.type, i.caption,
                   c.name as category_name
            FROM infos i
            JOIN infos_apartment_lnk ial ON i.id = ial.info_id
            JOIN infos_category_lnk icl ON i.id = icl.info_id
            JOIN categories c ON icl.category_id = c.id
            WHERE ial.apartment_id = $1 
            AND c.name LIKE 'Кастом %'
            AND i.name LIKE '%' || $2 || '%'
            ORDER BY i.created_at
        ''', apt_id, section_name)
        
        result = []
        for row in rows:
            result.append({
                'field_key': f"custom_{row['id']}",
                'field_name': row['field_name'],
                'text_content': row['text'],
                'file_id': row['caption'],
                'file_type': row['type']
            })
        
        return result

async def delete_custom_field(apt_id: int, section: str, field_key: str):
    """Удалить кастомное поле"""
    info_id = int(field_key.split('_')[1])
    
    async with db_pool.acquire() as conn:
        await conn.execute('DELETE FROM infos_apartment_lnk WHERE info_id = $1', info_id)
        await conn.execute('DELETE FROM infos_category_lnk WHERE info_id = $1', info_id)
        await conn.execute('DELETE FROM infos WHERE id = $1', info_id)

@dp.callback_query(F.data.startswith("add_custom_"))
async def add_custom_button_start(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    section = parts[2]
    apt_id = int(parts[3])
    
    await state.update_data(
        custom_section=section,
        custom_apartment_id=apt_id
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_{section}_{apt_id}")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"section_{section}_{apt_id}")]
    ])
    
    await callback.message.edit_text(
        "Введите название кнопки:",
        reply_markup=keyboard
    )
    await state.set_state(ApartmentStates.adding_custom_button_name)
    await callback.answer()

@dp.message(ApartmentStates.adding_custom_button_name)
async def process_custom_button_name(message: types.Message, state: FSMContext):
    data = await state.get_data()
    custom_name = message.text
    section = data['custom_section']
    apt_id = data['custom_apartment_id']
    
    await state.update_data(custom_button_name=custom_name)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_{section}_{apt_id}")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"section_{section}_{apt_id}")]
    ])
    
    await message.answer(
        "Введите содержимое кнопки:",
        reply_markup=keyboard
    )
    await state.set_state(ApartmentStates.adding_custom_button_content)

@dp.message(ApartmentStates.adding_custom_button_content)
async def process_custom_button_content(message: types.Message, state: FSMContext):
    data = await state.get_data()
    apt_id = data['custom_apartment_id']
    section = data['custom_section']
    field_name = data['custom_button_name']
    
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
    
    await state.update_data(
        custom_text_content=text_content,
        custom_file_id=file_id,
        custom_file_type=file_type
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"section_{section}_{apt_id}")],
        [InlineKeyboardButton(text="Сохранить", callback_data=f"save_custom_{section}_{apt_id}")],
        [InlineKeyboardButton(text="Не сохранять", callback_data=f"section_{section}_{apt_id}")]
    ])
    
    await message.answer(
        "Сохранить кнопку?",
        reply_markup=keyboard
    )
    await state.set_state(ApartmentStates.waiting_custom_confirm)

@dp.callback_query(F.data.startswith("save_custom_"))
async def save_custom_field(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    section = parts[2]
    apt_id = int(parts[3])
    
    data = await state.get_data()
    field_name = data['custom_button_name']
    text_content = data.get('custom_text_content')
    file_id = data.get('custom_file_id')
    file_type = data.get('custom_file_type')
    
    # Создаём кастомную категорию
    section_name = SECTION_TO_CATEGORY_MAP.get(section, section)
    custom_category_name = f"Кастом {field_name}"
    
    async with db_pool.acquire() as conn:
        # Создаём категорию
        cat_id = await conn.fetchval('''
            INSERT INTO categories (
                name, expandable, editable,
                created_at, updated_at, published_at
            )
            VALUES ($1, TRUE, TRUE, NOW(), NOW(), NOW())
            RETURNING id
        ''', custom_category_name)
        
        # Создаём info
        info_id = await conn.fetchval('''
            INSERT INTO infos (
                name, text, type, caption,
                created_at, updated_at, published_at
            )
            VALUES ($1, $2, $3, $4, NOW(), NOW(), NOW())
            RETURNING id
        ''', field_name, text_content, file_type or 'text', file_id)
        
        # Связываем с квартирой
        await conn.execute('''
            INSERT INTO infos_apartment_lnk (info_id, apartment_id)
            VALUES ($1, $2)
        ''', info_id, apt_id)
        
        # Связываем с категорией
        await conn.execute('''
            INSERT INTO infos_category_lnk (info_id, category_id)
            VALUES ($1, $2)
        ''', info_id, cat_id)
    
    # Показываем страницу кастомной кнопки
    field_key = f"custom_{info_id}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"section_{section}_{apt_id}")],
        [InlineKeyboardButton(text="Удалить кнопку", callback_data=f"delete_custom_{apt_id}_{section}_{field_key}")]
    ])
    
    preview_text = text_content[:50] + "..." if text_content and len(text_content) > 50 else text_content or "(контент)"
    
    text = f"Кастомная кнопка: {field_name}\n\n{preview_text}"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await state.clear()
    await callback.answer("✅ Кнопка сохранена!")

@dp.callback_query(F.data.startswith("delete_custom_"))
async def delete_custom_field_handler(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    apt_id = int(parts[2])
    section = parts[3]
    field_key = "_".join(parts[4:])
    
    await delete_custom_field(apt_id, section, field_key)
    
    filled_fields = await get_filled_fields(apt_id, section)
    
    if section == "checkin":
        filled_checkin = await get_filled_fields(apt_id, 'checkin')
        filled_help = await get_filled_fields(apt_id, 'help')
        filled_stores = await get_filled_fields(apt_id, 'stores')
        all_filled = filled_checkin | filled_help | filled_stores
        keyboard = await get_checkin_section_keyboard_async(apt_id, all_filled)
        text = "Раздел 🧳 Заселение"
    elif section == "rent":
        keyboard = get_rent_section_keyboard(apt_id, filled_fields)
        text = "Раздел 📹 Аренда"
    elif section == "experiences":
        keyboard = get_experiences_section_keyboard(apt_id, filled_fields)
        text = "Раздел 🍿 Впечатления"
    elif section == "checkout":
        keyboard = get_checkout_section_keyboard(apt_id, filled_fields)
        text = "Раздел 📦 Выселение"
    elif section == "help":
        keyboard = get_help_subsection_keyboard(apt_id, filled_fields)
        text = "Подраздел 🏠 Помощь"
    else:
        keyboard = get_stores_subsection_keyboard(apt_id, filled_fields)
        text = "Подраздел 📍 Магазины"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer("✅ Кнопка удалена!")

@dp.callback_query(F.data.startswith("custom_field_"))
async def view_custom_field(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    apt_id = int(parts[2])
    section = parts[3]
    field_key = "_".join(parts[4:])
    
    info_id = int(field_key.split('_')[1])
    
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT name, text, type, caption
            FROM infos
            WHERE id = $1
        ''', info_id)
    
    if not row:
        await callback.answer("Кнопка не найдена", show_alert=True)
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"section_{section}_{apt_id}")],
        [InlineKeyboardButton(text="Удалить кнопку", callback_data=f"delete_custom_{apt_id}_{section}_{field_key}")]
    ])
    
    text_content = row['text']
    preview_text = text_content[:50] + "..." if text_content and len(text_content) > 50 else text_content or "(контент)"
    
    text = f"Кастомная кнопка: {row['name']}\n\n{preview_text}"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# ============================================
# БРОНИРОВАНИЯ
# ============================================

@dp.callback_query(F.data.startswith("bookings_"))
async def bookings_menu(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[1])
    
    bookings = await get_apartment_bookings(apt_id)
    
    text = (
        "Список бронирований.\n\n"
        "Бронь нужно выдавать гостю для доступа к закрытой информации.\n"
        "После проживания завершите бронирование."
    )
    
    buttons = []
    
    for booking in bookings:
        guest_name = booking['guest_name']
        checkin = booking['checkin'].strftime('%d.%m.%y')
        icon = "🔴" if not booking['is_complete'] else "⚪"
        buttons.append([InlineKeyboardButton(
            text=f"{guest_name} — {checkin} {icon}",
            callback_data=f"view_booking_{booking['id']}"
        )])
    
    buttons.append([InlineKeyboardButton(text="➕ Добавить", callback_data=f"add_booking_{apt_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"apartment_{apt_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("add_booking_"))
async def add_booking(callback: types.CallbackQuery, state: FSMContext):
    apt_id = int(callback.data.split("_")[2])
    
    await state.update_data(booking_apartment_id=apt_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"bookings_{apt_id}")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"bookings_{apt_id}")]
    ])
    
    await callback.message.edit_text(
        "Введите ФИО гостя:",
        reply_markup=keyboard
    )
    await state.set_state(BookingStates.waiting_guest_name)
    await callback.answer()

@dp.message(BookingStates.waiting_guest_name)
async def process_guest_name(message: types.Message, state: FSMContext):
    await state.update_data(guest_name=message.text)
    
    data = await state.get_data()
    apt_id = data.get('booking_apartment_id')
    
    if not apt_id:
        await state.clear()
        await message.answer("❌ Ошибка", reply_markup=get_main_menu_keyboard())
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"bookings_{apt_id}")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"bookings_{apt_id}")]
    ])
    
    await message.answer(
        "Введите дату заезда в формате 20.06.2025",
        reply_markup=keyboard
    )
    await state.set_state(BookingStates.waiting_checkin_date)

@dp.message(BookingStates.waiting_checkin_date)
async def process_checkin_date(message: types.Message, state: FSMContext):
    data = await state.get_data()
    apt_id = data.get('booking_apartment_id')
    guest_name = data.get('guest_name')
    
    if not apt_id or not guest_name:
        await state.clear()
        await message.answer("❌ Ошибка", reply_markup=get_main_menu_keyboard())
        return
    
    try:
        checkin_date = datetime.strptime(message.text, '%d.%m.%Y').date()
        booking_id, hash_code = await create_booking(apt_id, guest_name, checkin_date)
        
        bot_username = (await bot.get_me()).username
        guest_link = f"https://t.me/{bot_username}?start=guest_{hash_code}"
        
        bookings = await get_apartment_bookings(apt_id)
        
        text = "Список бронирований"
        
        buttons = []
        for booking in bookings:
            b_guest_name = booking['guest_name']
            b_checkin = booking['checkin'].strftime('%d.%m.%y')
            icon = "🔴" if not booking['is_complete'] else "⚪"
            buttons.append([InlineKeyboardButton(
                text=f"{b_guest_name} — {b_checkin} {icon}",
                callback_data=f"view_booking_{booking['id']}"
            )])
        
        buttons.append([InlineKeyboardButton(text="➕ Добавить", callback_data=f"add_booking_{apt_id}")])
        buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"apartment_{apt_id}")])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await message.answer(text, reply_markup=keyboard)
        await message.answer(f"🔗 Ссылка для гостя:\n{guest_link}")
        await state.clear()
        
    except ValueError:
        await message.answer("Неверный формат даты. Используйте: 20.06.2025")

@dp.callback_query(F.data.startswith("view_booking_"))
async def view_booking(callback: types.CallbackQuery):
    booking_id = int(callback.data.split("_")[2])
    
    async with db_pool.acquire() as conn:
        booking = await conn.fetchrow('''
            SELECT b.*, bal.apartment_id
            FROM bookings b
            JOIN bookings_apartment_lnk bal ON b.id = bal.booking_id
            WHERE b.id = $1
        ''', booking_id)
    
    if not booking:
        await callback.answer("Бронирование не найдено", show_alert=True)
        return
    
    apt_id = booking['apartment_id']
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Завершить", callback_data=f"complete_booking_{booking_id}_{apt_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"bookings_{apt_id}")]
    ])
    
    text = f"Бронирование:\n\nГость: {booking['guest_name']}\nДата заезда: {booking['checkin']}"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("complete_booking_"))
async def complete_booking_handler(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    booking_id = int(parts[2])
    apt_id = int(parts[3]) if len(parts) > 3 else None
    
    await complete_booking(booking_id)
    
    if apt_id:
        bookings = await get_apartment_bookings(apt_id)
        
        text = "Бронирование завершено.\n\nСписок бронирований"
        
        buttons = []
        for booking in bookings:
            guest_name = booking['guest_name']
            checkin = booking['checkin'].strftime('%d.%m.%y')
            icon = "🔴" if not booking['is_complete'] else "⚪"
            buttons.append([InlineKeyboardButton(
                text=f"{guest_name} — {checkin} {icon}",
                callback_data=f"view_booking_{booking['id']}"
            )])
        
        buttons.append([InlineKeyboardButton(text="➕ Добавить", callback_data=f"add_booking_{apt_id}")])
        buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"apartment_{apt_id}")])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.edit_text(text, reply_markup=keyboard)
    else:
        await callback.message.edit_text("Бронирование завершено")
    
    await callback.answer("✅ Завершено")

@dp.callback_query(F.data.startswith("owner_link_"))
async def generate_owner_link(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[2])
    
    apt_info = await get_apartment_info(apt_id)
    apt_name = apt_info['name']
    
    bot_username = (await bot.get_me()).username
    owner_link = f"https://t.me/{bot_username}?start=owner_{apt_id}"
    
    text = f"Ссылка для собственника объекта «{apt_name}»:\n{owner_link}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"apartment_{apt_id}")]
    ])
    
    await callback.message.answer(text, reply_markup=keyboard)
    await callback.answer()

# ============================================
# ПРЕДПРОСМОТР ОБЪЕКТА
# ============================================

@dp.callback_query(F.data.startswith("apt_preview_"))
async def preview_apartment(callback: types.CallbackQuery, state: FSMContext):
    apt_id = int(callback.data.split("_")[2])
    
    await state.update_data(preview_mode=True, preview_apartment_id=apt_id)
    
    apt_info = await get_apartment_info(apt_id)
    apt_name = apt_info['name']
    address = apt_info['address'] or "Москва"
    
    text = f"{apt_name}\n\nАдрес: {address}.\n\nИнформация для изучения:"
    
    buttons = []
    buttons.append([InlineKeyboardButton(text="➡️ Начать", callback_data=f"prevw_start_{apt_id}")])
    buttons.append([InlineKeyboardButton(text="🚕 Такси", url="https://taxi.yandex.ru")])
    buttons.append([InlineKeyboardButton(text="Режим владельца", callback_data=f"exit_preview_{apt_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("prevw_start_"))
async def preview_start(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[2])
    
    apt_info = await get_apartment_info(apt_id)
    apt_name = apt_info['name']
    
    async with db_pool.acquire() as conn:
        sections_data = await conn.fetch('''
            SELECT DISTINCT c.name as section_name
            FROM infos i
            JOIN infos_apartment_lnk ial ON i.id = ial.info_id
            JOIN infos_category_lnk icl ON i.id = icl.info_id
            JOIN categories c ON icl.category_id = c.id
            WHERE ial.apartment_id = $1
        ''', apt_id)
    
    available_sections = set(row['section_name'] for row in sections_data)
    
    buttons = []
    if 'Аренда' in available_sections:
        buttons.append([InlineKeyboardButton(text="📹 Аренда", callback_data=f"prevw_section_rent_{apt_id}")])
    if 'Заселение' in available_sections:
        buttons.append([InlineKeyboardButton(text="🧳 Заселение", callback_data=f"prevw_section_checkin_{apt_id}")])
    if 'Впечатления' in available_sections:
        buttons.append([InlineKeyboardButton(text="🍿 Впечатления", callback_data=f"prevw_section_experiences_{apt_id}")])
    if 'Выселение' in available_sections:
        buttons.append([InlineKeyboardButton(text="📦 Выселение", callback_data=f"prevw_section_checkout_{apt_id}")])
    
    buttons.append([InlineKeyboardButton(text="Режим владельца", callback_data=f"exit_preview_{apt_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    text = f"{apt_name}\n\nИнформация:"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("prevw_section_"))
async def preview_section(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    section = parts[2]
    apt_id = int(parts[3])
    
    fields = await get_section_fields(apt_id, section)
    
    section_name = SECTION_NAMES.get(section, section)
    section_icon = SECTION_ICONS.get(section, "📄")
    
    text = f"Раздел {section_icon} {section_name}"
    
    buttons = []
    
    for field in fields:
        field_name = field['field_name']
        buttons.append([InlineKeyboardButton(text=field_name, callback_data=f"prevw_field_{apt_id}_{section}_{field['field_key']}")])
    
    if section == 'checkin':
        help_fields = await get_section_fields(apt_id, 'help')
        if help_fields:
            buttons.append([InlineKeyboardButton(text="🏠 Помощь", callback_data=f"prevw_subsection_help_{apt_id}")])
        
        stores_fields = await get_section_fields(apt_id, 'stores')
        if stores_fields:
            buttons.append([InlineKeyboardButton(text="📍 Магазины", callback_data=f"prevw_subsection_stores_{apt_id}")])
    
    if not buttons:
        await callback.answer("Раздел пуст", show_alert=True)
        return
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"prevw_start_{apt_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("prevw_subsection_help_"))
async def preview_subsection_help(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[3])
    
    fields = await get_section_fields(apt_id, 'help')
    
    if not fields:
        await callback.answer("Подраздел пуст", show_alert=True)
        return
    
    text = "Подраздел 🏠 Помощь"
    
    buttons = []
    for field in fields:
        field_name = field['field_name']
        buttons.append([InlineKeyboardButton(text=field_name, callback_data=f"prevw_field_{apt_id}_help_{field['field_key']}")])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"prevw_section_checkin_{apt_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("prevw_subsection_stores_"))
async def preview_subsection_stores(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[3])
    
    fields = await get_section_fields(apt_id, 'stores')
    
    if not fields:
        await callback.answer("Подраздел пуст", show_alert=True)
        return
    
    text = "Подраздел 📍 Магазины"
    
    buttons = []
    for field in fields:
        field_name = field['field_name']
        buttons.append([InlineKeyboardButton(text=field_name, callback_data=f"prevw_field_{apt_id}_stores_{field['field_key']}")])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"prevw_section_checkin_{apt_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("prevw_field_"))
async def preview_field(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    apt_id = int(parts[2])
    section = parts[3]
    field_key = "_".join(parts[4:])
    
    field_data = await get_apartment_field(apt_id, section, field_key)
    
    if not field_data:
        await callback.answer("Нет данных", show_alert=True)
        return
    
    field_name = FIELD_NAMES.get(field_key, "Поле")
    
    text_content = field_data['text_content']
    file_id = field_data['file_id']
    file_type = field_data['file_type']
    
    header = f"Поле: {field_name}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"prevw_section_{section}_{apt_id}")]
    ])
    
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
            full_text = f"{header}\n\n{text_content}" if text_content else header
            await callback.message.edit_text(full_text, reply_markup=keyboard)
    elif text_content:
        full_text = f"{header}\n\n{text_content}"
        await callback.message.edit_text(full_text, reply_markup=keyboard)
    else:
        await callback.message.edit_text(header, reply_markup=keyboard)
    
    await callback.answer()

@dp.callback_query(F.data.startswith("exit_preview_"))
async def exit_preview(callback: types.CallbackQuery, state: FSMContext):
    apt_id = int(callback.data.split("_")[2])
    await state.update_data(preview_mode=False)
    
    apt_info = await get_apartment_info(apt_id)
    apt_name = apt_info['name']
    is_long = apt_info['is_long']
    
    text = f"Объект: {apt_name}"
    await callback.message.edit_text(text, reply_markup=get_apartment_menu_keyboard(apt_id, is_long))
    await callback.answer()

# ============================================
# РЕЖИМ ГОСТЯ
# ============================================

@dp.callback_query(F.data.startswith("guest_start_"))
async def guest_start(callback: types.CallbackQuery, state: FSMContext):
    apt_id = int(callback.data.split("_")[2])
    
    await state.update_data(guest_mode=True, guest_apartment_id=apt_id)
    
    apt_info = await get_apartment_info(apt_id)
    apt_name = apt_info['name']
    
    async with db_pool.acquire() as conn:
        sections_data = await conn.fetch('''
            SELECT DISTINCT c.name as section_name
            FROM infos i
            JOIN infos_apartment_lnk ial ON i.id = ial.info_id
            JOIN infos_category_lnk icl ON i.id = icl.info_id
            JOIN categories c ON icl.category_id = c.id
            WHERE ial.apartment_id = $1
        ''', apt_id)
    
    available_sections = set(row['section_name'] for row in sections_data)
    
    buttons = []
    if 'Аренда' in available_sections:
        buttons.append([InlineKeyboardButton(text="📹 Аренда", callback_data=f"guest_section_rent_{apt_id}")])
    if 'Заселение' in available_sections:
        buttons.append([InlineKeyboardButton(text="🧳 Заселение", callback_data=f"guest_section_checkin_{apt_id}")])
    if 'Впечатления' in available_sections:
        buttons.append([InlineKeyboardButton(text="🍿 Впечатления", callback_data=f"guest_section_experiences_{apt_id}")])
    if 'Выселение' in available_sections:
        buttons.append([InlineKeyboardButton(text="📦 Выселение", callback_data=f"guest_section_checkout_{apt_id}")])
    
    buttons.append([InlineKeyboardButton(text="Режим владельца", callback_data="switch_to_owner")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    text = f"{apt_name}\n\nИнформация:"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("guest_section_"))
async def guest_view_section(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    section = parts[2]
    apt_id = int(parts[3])
    
    fields = await get_section_fields(apt_id, section)
    
    section_name = SECTION_NAMES.get(section, section)
    section_icon = SECTION_ICONS.get(section, "📄")
    
    if section == 'checkin':
        text = f"Раздел {section_icon} {section_name} ❤️"
    else:
        text = f"Раздел {section_icon} {section_name}"
    
    buttons = []
    
    for field in fields:
        field_name = field['field_name']
        buttons.append([InlineKeyboardButton(text=field_name, callback_data=f"guest_field_{apt_id}_{section}_{field['field_key']}")])
    
    if section == 'checkin':
        help_fields = await get_section_fields(apt_id, 'help')
        if help_fields:
            buttons.append([InlineKeyboardButton(text="🏠 Помощь", callback_data=f"guest_subsection_help_{apt_id}")])
        
        stores_fields = await get_section_fields(apt_id, 'stores')
        if stores_fields:
            buttons.append([InlineKeyboardButton(text="📍 Магазины", callback_data=f"guest_subsection_stores_{apt_id}")])
    
    if not buttons:
        await callback.answer("Раздел пуст", show_alert=True)
        return
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"guest_start_{apt_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("guest_subsection_help_"))
async def guest_subsection_help(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[3])
    
    fields = await get_section_fields(apt_id, 'help')
    
    if not fields:
        await callback.answer("Подраздел пуст", show_alert=True)
        return
    
    text = "Подраздел 🏠 Помощь"
    
    buttons = []
    for field in fields:
        field_name = field['field_name']
        buttons.append([InlineKeyboardButton(text=field_name, callback_data=f"guest_field_{apt_id}_help_{field['field_key']}")])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"guest_section_checkin_{apt_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("guest_subsection_stores_"))
async def guest_subsection_stores(callback: types.CallbackQuery):
    apt_id = int(callback.data.split("_")[3])
    
    fields = await get_section_fields(apt_id, 'stores')
    
    if not fields:
        await callback.answer("Подраздел пуст", show_alert=True)
        return
    
    text = "Подраздел 📍 Магазины"
    
    buttons = []
    for field in fields:
        field_name = field['field_name']
        buttons.append([InlineKeyboardButton(text=field_name, callback_data=f"guest_field_{apt_id}_stores_{field['field_key']}")])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"guest_section_checkin_{apt_id}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("guest_field_"))
async def guest_view_field(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    apt_id = int(parts[2])
    section = parts[3]
    field_key = "_".join(parts[4:])
    
    field_data = await get_apartment_field(apt_id, section, field_key)
    
    if not field_data:
        await callback.answer("Нет данных", show_alert=True)
        return
    
    field_name = FIELD_NAMES.get(field_key, "Поле")
    
    text_content = field_data['text_content']
    file_id = field_data['file_id']
    file_type = field_data['file_type']
    
    header = f"Поле: {field_name}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"guest_section_{section}_{apt_id}")]
    ])
    
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
            full_text = f"{header}\n\n{text_content}" if text_content else header
            await callback.message.edit_text(full_text, reply_markup=keyboard)
    elif text_content:
        full_text = f"{header}\n\n{text_content}"
        await callback.message.edit_text(full_text, reply_markup=keyboard)
    else:
        await callback.message.edit_text(header, reply_markup=keyboard)
    
    await callback.answer()

@dp.callback_query(F.data == "switch_to_owner")
async def switch_to_owner_mode(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    
    organizations = await get_manager_organizations(callback.from_user.id)
    
    if organizations:
        await state.update_data(current_organization_id=organizations[0][0])
        await callback.message.edit_text(
            "Главное меню",
            reply_markup=get_main_menu_keyboard()
        )
    else:
        await callback.message.edit_text(
            "Создайте компанию",
            reply_markup=get_add_organization_keyboard()
        )
    
    await callback.answer("Режим владельца")

# ============================================
# СИСТЕМА ПРЕДЛОЖЕНИЙ
# ============================================

@dp.callback_query(F.data == "suggest_improvement")
async def suggest_improvement_start(callback: types.CallbackQuery, state: FSMContext):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])
    
    text = (
        "💡 Предложить улучшение бота\n\n"
        "Напишите что бы вы хотели улучшить в боте"
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await state.set_state(SuggestionStates.waiting_suggestion)
    await callback.answer()

@dp.message(SuggestionStates.waiting_suggestion)
async def process_suggestion(message: types.Message, state: FSMContext):
    suggestion_text = message.text
    
    if len(suggestion_text) < 10:
        await message.answer(
            "⚠️ Предложение слишком короткое. Минимум 10 символов.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Отмена", callback_data="main_menu")]
            ])
        )
        return
    
    if len(suggestion_text) > 1000:
        await message.answer(
            "⚠️ Предложение слишком длинное. Максимум 1000 символов.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Отмена", callback_data="main_menu")]
            ])
        )
        return
    
    # Сохраняем в лог
    logger.info(f"💡 Suggestion from {message.from_user.id} ({message.from_user.username}): {suggestion_text}")
    
    await message.answer(
        "✅ Спасибо за предложение!\n\nВаше сообщение отправлено разработчикам.",
        reply_markup=get_main_menu_keyboard()
    )
    
    await state.clear()

# ============================================
# ДОПОЛНИТЕЛЬНЫЕ КОМАНДЫ
# ============================================

@dp.message(Command("company"))
async def cmd_company(message: types.Message, state: FSMContext):
    """Команда /company"""
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    if not org_id:
        organizations = await get_manager_organizations(message.from_user.id)
        if organizations:
            org_id = organizations[0][0]
            await state.update_data(current_organization_id=org_id)
        else:
            await message.answer(
                "Создайте компанию",
                reply_markup=get_add_organization_keyboard()
            )
            return
    
    org_info = await get_organization_info(org_id)
    
    if org_info:
        text = (
            f"{org_info['name']}\n"
            f"{org_info['city']}\n\n"
            f"Приветствие:\n{org_info.get('greeting', '')}"
        )
        await message.answer(text, reply_markup=get_organization_cabinet_keyboard(org_info))
    else:
        await message.answer(
            "Компания не найдена",
            reply_markup=get_add_organization_keyboard()
        )

@dp.message(Command("apartments"))
async def cmd_apartments(message: types.Message, state: FSMContext):
    """Команда /apartments"""
    data = await state.get_data()
    org_id = data.get('current_organization_id')
    
    if not org_id:
        organizations = await get_manager_organizations(message.from_user.id)
        if organizations:
            org_id = organizations[0][0]
            await state.update_data(current_organization_id=org_id)
        else:
            await message.answer(
                "Создайте компанию",
                reply_markup=get_add_organization_keyboard()
            )
            return
    
    apartments = await get_organization_apartments(org_id)
    
    if apartments:
        count_text = f"📊 Всего объектов: {len(apartments)}"
    else:
        count_text = "📭 Нет объектов"
    
    await message.answer(
        f"Список объектов\n\n{count_text}",
        reply_markup=get_apartments_list_keyboard(apartments)
    )

# ============================================
# FALLBACK HANDLERS
# ============================================

@dp.callback_query()
async def fallback_callback_handler(callback: types.CallbackQuery):
    logger.warning(f"⚠️ Unhandled callback: {callback.data}")
    
    await callback.answer(
        "⚠️ Кнопка устарела. Используйте /start",
        show_alert=True
    )
    
    try:
        await callback.message.edit_text(
            "Главное меню",
            reply_markup=get_main_menu_keyboard()
        )
    except Exception as e:
        logger.error(f"Failed to edit message: {e}")

@dp.message()
async def fallback_message_handler(message: types.Message):
    logger.warning(f"⚠️ Unhandled message: {message.text}")
    
    await message.answer(
        "⚠️ Используйте /start",
        reply_markup=get_main_menu_keyboard()
    )

# ============================================
# ЗАПУСК БОТА
# ============================================

async def on_shutdown():
    logger.info("Shutting down...")
    if db_pool:
        await db_pool.close()
    await bot.session.close()

async def main():
    try:
        await init_db()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return
    
    logger.info("✅ Bot started successfully")
    
    # HTTP сервер для health checks
    from aiohttp import web
    
    async def health_check(request):
        return web.Response(text="Bot is running")
    
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.getenv("PORT", "8080")))
    
    await site.start()
    logger.info(f"✅ Health check server started")
    
    # Настройка команд
    from aiogram.types import BotCommand
    
    commands = [
        BotCommand(command="start", description="🚀 Запустить"),
        BotCommand(command="home", description="🏡 Главная"),
        BotCommand(command="menu", description="🏠 Меню")
    ]
    
    try:
        await bot.set_my_commands(commands)
        logger.info("✅ Commands set")
    except Exception as e:
        logger.error(f"⚠️ Failed to set commands: {e}")
    
    # Запуск polling
    try:
        logger.info("🚀 Starting polling...")
        
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=True
        )
        
    except Exception as e:
        logger.error(f"❌ Polling error: {e}")
    finally:
        await on_shutdown()
        await runner.cleanup()
        logger.info("👋 Bot stopped")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped by user")
