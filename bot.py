import asyncio
import logging
import os
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
import asyncpg
from datetime import datetime
from typing import Optional

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Токен бота
BOT_TOKEN = os.getenv("BOT_TOKEN", "8376900263:AAGQLHq9dveqe_polSjWzw8UBfVVrV0eh0A")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:PECPoXHNBUxpIFYoQXVrQaLqSqpRbSYk@postgres.railway.internal:5432/railway")

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Глобальный пул соединений
db_pool: Optional[asyncpg.Pool] = None

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
        
        # Таблица информации по объектам (разделы)
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
        
        logger.info("Database initialized successfully")

# Состояния FSM
class CompanyStates(StatesGroup):
    waiting_company_name = State()
    waiting_company_city = State()
    waiting_welcome_message = State()
    waiting_timezone = State()
    waiting_checkin_time = State()
    waiting_checkout_time = State()

class PropertyStates(StatesGroup):
    waiting_property_name = State()
    waiting_property_address = State()
    editing_field = State()

# Вспомогательные функции для работы с БД
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
    
    async with db_pool.acquire() as conn:
        company_id = await conn.fetchval('''
            INSERT INTO companies (name, city, welcome_message)
            VALUES ($1, $2, $3)
            RETURNING id
        ''', name, city, welcome_msg)
        
        await conn.execute('''
            INSERT INTO user_companies (user_id, company_id, is_admin)
            VALUES ($1, $2, TRUE)
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
        row = await conn.fetchrow('''
            SELECT text_content, file_id, file_type
            FROM property_info
            WHERE property_id = $1 AND section = $2 AND field_key = $3
        ''', property_id, section, field_key)
        return row if row else None

async def get_property_sections_data(property_id: int):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT section, field_name, text_content, file_id, file_type
            FROM property_info
            WHERE property_id = $1
            ORDER BY section, field_name
        ''', property_id)
        return rows

async def delete_property(property_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute('DELETE FROM properties WHERE id = $1', property_id)

async def toggle_short_term(property_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute('''
            UPDATE properties 
            SET is_short_term = NOT is_short_term 
            WHERE id = $1
        ''', property_id)

async def get_property_name(property_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchval('SELECT name FROM properties WHERE id = $1', property_id)

# Клавиатуры
def get_main_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Добавление и настройка объектов", callback_data="objects_menu")],
        [InlineKeyboardButton(text="🏢 Личный кабинет компании", callback_data="company_cabinet")],
        [InlineKeyboardButton(text="🔌 Подключить шахматку", callback_data="connect_calendar")],
        [InlineKeyboardButton(text="📱 Проверка гостя", callback_data="guest_check")],
        [InlineKeyboardButton(text="💡 Что улучшить в боте?", callback_data="feedback")]
    ])

def get_add_company_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Добавить компанию", callback_data="add_company")]
    ])

def get_back_keyboard(callback="back"):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=callback)]
    ])

def get_objects_list_keyboard(properties):
    buttons = []
    for prop_id, name, address, is_short_term in properties:
        buttons.append([InlineKeyboardButton(text=name, callback_data=f"property_{prop_id}")])
    
    buttons.append([InlineKeyboardButton(text="➕ Добавить объект", callback_data="add_property")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_property_menu_keyboard(property_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧳 Заселение", callback_data=f"section_checkin_{property_id}")],
        [InlineKeyboardButton(text="📹 Аренда", callback_data=f"section_rent_{property_id}")],
        [InlineKeyboardButton(text="🍿 Впечатления", callback_data=f"section_experiences_{property_id}")],
        [InlineKeyboardButton(text="📦 Выселение", callback_data=f"section_checkout_{property_id}")],
        [InlineKeyboardButton(text="🔗 Бронирования", callback_data=f"bookings_{property_id}")],
        [InlineKeyboardButton(text="📅 Долгосрок", callback_data=f"toggle_shortterm_{property_id}")],
        [InlineKeyboardButton(text="Ссылка на объект для собственника", callback_data=f"owner_link_{property_id}")],
        [InlineKeyboardButton(text="Редактировать объект", callback_data=f"edit_property_{property_id}")],
        [InlineKeyboardButton(text="Предпросмотр объекта", callback_data=f"preview_{property_id}")],
        [InlineKeyboardButton(text="Удалить объект", callback_data=f"delete_property_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="objects_menu")]
    ])

def get_checkin_section_keyboard(property_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🕐 Время заселения и выселения", callback_data=f"field_checkin_time_{property_id}")],
        [InlineKeyboardButton(text="🚗 Парковка", callback_data=f"field_parking_{property_id}")],
        [InlineKeyboardButton(text="🌐 Wi-Fi", callback_data=f"field_wifi_{property_id}")],
        [InlineKeyboardButton(text="🔑 Ключ от двери", callback_data=f"field_door_key_{property_id}")],
        [InlineKeyboardButton(text="🗺 Как найти объект?", callback_data=f"field_how_to_find_{property_id}")],
        [InlineKeyboardButton(text="🚶 Как дойти до квартиры", callback_data=f"field_how_to_reach_{property_id}")],
        [InlineKeyboardButton(text="📄 Документы для заселения", callback_data=f"field_documents_{property_id}")],
        [InlineKeyboardButton(text="💰 Депозит", callback_data=f"field_deposit_{property_id}")],
        [InlineKeyboardButton(text="🔐 Дистанционное заселение", callback_data=f"field_remote_checkin_{property_id}")],
        [InlineKeyboardButton(text="🏠 Помощь с проживанием", callback_data=f"subsection_help_{property_id}")],
        [InlineKeyboardButton(text="📍 Магазины, аптеки итд.", callback_data=f"field_stores_{property_id}")],
        [InlineKeyboardButton(text="📢 Правила проживания", callback_data=f"field_rules_{property_id}")],
        [InlineKeyboardButton(text="➕ Добавить кнопку", callback_data=f"add_custom_checkin_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"property_{property_id}")]
    ])

def get_help_subsection_keyboard(property_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🥐 Завтрак", callback_data=f"field_breakfast_{property_id}")],
        [InlineKeyboardButton(text="🛏 Поменять бельё", callback_data=f"field_linen_{property_id}")],
        [InlineKeyboardButton(text="📱 Связь с менеджером", callback_data=f"field_manager_contact_{property_id}")],
        [InlineKeyboardButton(text="📺 Настройка ТВ", callback_data=f"field_tv_setup_{property_id}")],
        [InlineKeyboardButton(text="❄️ Кондиционер", callback_data=f"field_ac_{property_id}")],
        [InlineKeyboardButton(text="➕ Добавить кнопку", callback_data=f"add_custom_help_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_checkin_{property_id}")]
    ])

def get_experiences_section_keyboard(property_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗿 Экскурсии", callback_data=f"field_excursions_{property_id}")],
        [InlineKeyboardButton(text="🏛 Музеи", callback_data=f"field_museums_{property_id}")],
        [InlineKeyboardButton(text="🌳 Парки", callback_data=f"field_parks_{property_id}")],
        [InlineKeyboardButton(text="🎬 Кино и театры", callback_data=f"field_entertainment_{property_id}")],
        [InlineKeyboardButton(text="➕ Добавить кнопку", callback_data=f"add_custom_exp_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"property_{property_id}")]
    ])

def get_checkout_section_keyboard(property_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚪 Как выехать без менеджера?", callback_data=f"field_self_checkout_{property_id}")],
        [InlineKeyboardButton(text="💸 Возврат депозита", callback_data=f"field_deposit_return_{property_id}")],
        [InlineKeyboardButton(text="📅 Продлить проживание", callback_data=f"field_extend_stay_{property_id}")],
        [InlineKeyboardButton(text="🎁 Скидки", callback_data=f"field_discounts_{property_id}")],
        [InlineKeyboardButton(text="➕ Добавить кнопку", callback_data=f"add_custom_checkout_{property_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"property_{property_id}")]
    ])

def get_field_edit_keyboard(property_id: int, section: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_{section}_{property_id}")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"skip_field_{section}_{property_id}")]
    ])

# Маппинг названий полей
FIELD_NAMES = {
    'checkin_time': 'Время заселения и выселения',
    'parking': 'Парковка',
    'wifi': 'Wi-Fi',
    'door_key': 'Ключ от двери',
    'how_to_find': 'Как найти объект?',
    'how_to_reach': 'Как дойти до квартиры',
    'documents': 'Документы для заселения',
    'deposit': 'Депозит',
    'remote_checkin': 'Дистанционное заселение',
    'stores': 'Магазины, аптеки итд.',
    'rules': 'Правила проживания',
    'breakfast': 'Завтрак',
    'linen': 'Поменять бельё',
    'manager_contact': 'Связь с менеджером',
    'tv_setup': 'Настройка ТВ',
    'ac': 'Кондиционер',
    'excursions': 'Экскурсии',
    'museums': 'Музеи',
    'parks': 'Парки',
    'entertainment': 'Кино и театры',
    'self_checkout': 'Как выехать без менеджера?',
    'deposit_return': 'Возврат депозита',
    'extend_stay': 'Продлить проживание',
    'discounts': 'Скидки'
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
    'stores': 'Информация о действия гостя в случае ЧП. Здесь вы можете оставить контактные данные или инструкции на такой случай',
    'rules': 'Здесь вы можете добавить информацию и залоге и правилах, при которых он будет возвращён',
    'breakfast': 'Расскажите, возможен ли заказ завтрака в апартаменты и укажите стоимость для этой услуги',
    'linen': 'Укажите, возможность замены белья в апартаментах и стоимость этой услуги',
    'manager_contact': 'Информация о действия гостя в случае ЧП. Здесь вы можете оставить контактные данные или инструкции на такой случай',
    'tv_setup': 'Здесь можно упомянуть возможности и особенности вашего телевизора',
    'ac': 'Например: где находится пульт, что делать если кондиционер не работает',
}

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
    
    companies = await get_user_companies(user_id)
    
    if not companies:
        text = (
            "Добро пожаловать в #ботподелу.\n\n"
            "Для того, чтобы пользоваться ботом, вам необходимо выбрать компанию. "
            "Если ваши коллеги уже создали компанию, необходимо, чтобы они поделились с вами пригласительной ссылкой.\n\n"
            "Если вы хотите создать свою компанию, нажмите на кнопку «Добавить компанию».\n\n"
            "К этому сообщению мы прикрепили подробную инструкцию как пользоваться ботом. "
            "Вы сможете вернуться к ней позже, если потребуется."
        )
        await message.answer(text, reply_markup=get_add_company_keyboard())
    else:
        await state.update_data(current_company_id=companies[0][0])
        text = (
            "Вы в главном меню бота 🏠\n\n"
            "Если вы хотите добавить апартаменты и поделиться ссылкой с гостями, "
            "переходите в раздел «Добавление и настройка объектов»\n\n"
            "Если вы хотите изменить общие настройки компании или её название и город, "
            "переходите в раздел «Личный кабинет компании»\n\n"
            "В разделе «Проверка гостя», вы получаете доступ к возможности проверить гостя "
            "по нескольким открытым базам данных, а также добавить свой отзыв."
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
    company_name = data['company_name']
    company_city = message.text
    
    company_id = await create_company(company_name, company_city, message.from_user.id)
    await state.update_data(current_company_id=company_id)
    
    await message.answer(
        f"Отлично! Компания создана.\n\nНазвание: {company_name}\nГород: {company_city}",
        reply_markup=get_main_menu_keyboard()
    )
    await state.clear()

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

# Меню объектов
@dp.callback_query(F.data == "objects_menu")
async def objects_menu(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    
    if not company_id:
        await callback.answer("Ошибка: компания не выбрана", show_alert=True)
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
    property_name = data['property_name']
    property_address = message.text
    company_id = data.get('current_company_id')
    
    property_id = await create_property(company_id, property_name, property_address)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 Сохранить", callback_data=f"confirm_save_{property_id}")],
        [InlineKeyboardButton(text="❌ Не сохранять", callback_data="objects_menu")]
    ])
    
    await message.answer("Сохранить объект?", reply_markup=keyboard)
    await state.clear()

@dp.callback_query(F.data.startswith("confirm_save_"))
async def confirm_save(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    company_id = data.get('current_company_id')
    properties = await get_company_properties(company_id)
    
    await callback.message.edit_text(
        "Вот список ваших объектов. Здесь вы можете добавлять и редактировать их.",
        reply_markup=get_objects_list_keyboard(properties)
    )
    await callback.answer("Объект сохранен!")

@dp.callback_query(F.data == "skip_address")
async def skip_address(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    property_name = data['property_name']
    company_id = data.get('current_company_id')
    
    property_id = await create_property(company_id, property_name, "")
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 Сохранить", callback_data=f"confirm_save_{property_id}")],
        [InlineKeyboardButton(text="❌ Не сохранять", callback_data="objects_menu")]
    ])
    
    await callback.message.edit_text("Сохранить объект?", reply_markup=keyboard)
    await state.clear()
    await callback.answer()

# Просмотр объекта
@dp.callback_query(F.data.startswith("property_"))
async def view_property(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[1])
    property_name = await get_property_name(property_id)
    
    if property_name:
        text = f"Вы на странице объекта {property_name}.\n\nТут вы можете отредактировать информацию о объекте, которая будет доступна гостям."
        await callback.message.edit_text(text, reply_markup=get_property_menu_keyboard(property_id))
    
    await callback.answer()

# Разделы объекта
@dp.callback_query(F.data.startswith("section_checkin_"))
async def section_checkin(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    await callback.message.edit_text(
        "Вы на странице категории 🧳 Заселение",
        reply_markup=get_checkin_section_keyboard(property_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("subsection_help_"))
async def subsection_help(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    await callback.message.edit_text(
        "Вы на странице категории 🏠 Помощь с проживанием",
        reply_markup=get_help_subsection_keyboard(property_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("section_experiences_"))
async def section_experiences(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    await callback.message.edit_text(
        "Раздел: Впечатления",
        reply_markup=get_experiences_section_keyboard(property_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("section_checkout_"))
async def section_checkout(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    await callback.message.edit_text(
        "Раздел: Выселение",
        reply_markup=get_checkout_section_keyboard(property_id)
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
    
    text = f"Вы редактируете кнопку\n{field_desc}\nМожно добавить текст, фото, видео или документ\n\nВведите содержимое кнопки:"
    
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
    
    # Возвращаемся в раздел
    if section == "help":
        keyboard = get_help_subsection_keyboard(property_id)
        text = "Вы на странице категории 🏠 Помощь с проживанием"
    elif section == "experiences":
        keyboard = get_experiences_section_keyboard(property_id)
        text = "Раздел: Впечатления"
    elif section == "checkout":
        keyboard = get_checkout_section_keyboard(property_id)
        text = "Раздел: Выселение"
    else:
        keyboard = get_checkin_section_keyboard(property_id)
        text = "Вы на странице категории 🧳 Заселение"
    
    await message.answer(text, reply_markup=keyboard)
    await state.clear()

@dp.callback_query(F.data.startswith("skip_field_"))
async def skip_field(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    section = parts[2]
    property_id = int(parts[3])
    
    if section == "help":
        keyboard = get_help_subsection_keyboard(property_id)
        text = "Вы на странице категории 🏠 Помощь с проживанием"
    elif section == "experiences":
        keyboard = get_experiences_section_keyboard(property_id)
        text = "Раздел: Впечатления"
    elif section == "checkout":
        keyboard = get_checkout_section_keyboard(property_id)
        text = "Раздел: Выселение"
    else:
        keyboard = get_checkin_section_keyboard(property_id)
        text = "Вы на странице категории 🧳 Заселение"
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await state.clear()
    await callback.answer()

# Предпросмотр объекта
@dp.callback_query(F.data.startswith("preview_"))
async def preview_property(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[1])
    property_name = await get_property_name(property_id)
    
    sections_data = await get_property_sections_data(property_id)
    
    if not sections_data:
        await callback.answer("Нет данных для предпросмотра", show_alert=True)
        return
    
    # Группируем по секциям
    sections = {}
    for row in sections_data:
        section = row['section']
        if section not in sections:
            sections[section] = []
        sections[section].append(row)
    
    text = f"Предпросмотр объекта: {property_name}\n\n"
    
    section_names = {
        'checkin': '🧳 Заселение',
        'help': '🏠 Помощь с проживанием',
        'experiences': '🍿 Впечатления',
        'checkout': '📦 Выселение'
    }
    
    for section_key, items in sections.items():
        text += f"\n{section_names.get(section_key, section_key)}:\n"
        for item in items:
            text += f"• {item['field_name']}: "
            if item['text_content']:
                text += item['text_content'][:50] + ("..." if len(item['text_content']) > 50 else "")
            if item['file_id']:
                text += f" [{item['file_type']}]"
            text += "\n"
    
    await callback.message.answer(text)
    await callback.answer()

# Удаление объекта
@dp.callback_query(F.data.startswith("delete_property_"))
async def confirm_delete_property(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_delete_{property_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"property_{property_id}")]
    ])
    
    await callback.message.edit_text(
        "Вы точно хотите удалить объект?",
        reply_markup=keyboard
    )
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
async def toggle_shortterm(callback: types.CallbackQuery):
    property_id = int(callback.data.split("_")[2])
    await toggle_short_term(property_id)
    
    await callback.answer("Режим переключен")
    await view_property(callback)

# Заглушки
@dp.callback_query(F.data.in_(["connect_calendar", "guest_check", "feedback", "company_cabinet"]))
async def placeholder(callback: types.CallbackQuery):
    await callback.answer("Функция в разработке", show_alert=True)

# Запуск бота
async def main():
    await init_db()
    logger.info("Bot started")
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
