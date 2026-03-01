import telebot
import requests
import sqlite3
import time
import threading
import re
from datetime import datetime, timedelta
from telebot import types
from bs4 import BeautifulSoup
import json
import random
import os
import pickle
from pathlib import Path

TOKEN = '8596066830:AAGLyPOd7MDeuojUduvA_vIhX9YtP5rWTv4'
DB_NAME = 'games.db'
CHECK_INTERVAL = 3
ADMIN_IDS = [1346779759]

BASE_DIR = Path(__file__).parent
USERS_DIR = BASE_DIR / 'users'
CACHE_DIR = BASE_DIR / 'cache'
LOGS_DIR = BASE_DIR / 'logs'
ERRORS_DIR = BASE_DIR / 'errors'
BACKUP_DIR = BASE_DIR / 'backups'

for directory in [USERS_DIR, CACHE_DIR, LOGS_DIR, ERRORS_DIR, BACKUP_DIR]:
    directory.mkdir(exist_ok=True)

bot = telebot.TeleBot(TOKEN)

# ==================== ФУНКЦИЯ ИНИЦИАЛИЗАЦИИ БАЗЫ ДАННЫХ ====================

def init_database():
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS users
                     (user_id INTEGER PRIMARY KEY, 
                      username TEXT,
                      first_name TEXT,
                      last_name TEXT,
                      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                      last_activity TIMESTAMP)''')
        
        c.execute("PRAGMA table_info(users)")
        existing_columns = [column[1] for column in c.fetchall()]
        
        required_columns = {
            'first_name': 'TEXT',
            'last_name': 'TEXT',
            'last_activity': 'TIMESTAMP'
        }
        
        for column, col_type in required_columns.items():
            if column not in existing_columns:
                try:
                    c.execute(f"ALTER TABLE users ADD COLUMN {column} {col_type}")
                    print(f"✅ Добавлена колонка {column}")
                except Exception as e:
                    print(f"⚠️ Не удалось добавить колонку {column}: {e}")
        
        conn.commit()
        conn.close()
        print("✅ База данных инициализирована и проверена")
        
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute("PRAGMA table_info(users)")
        columns = c.fetchall()
        print("📊 Структура таблицы users:")
        for col in columns:
            print(f"   • {col[1]} ({col[2]})")
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")

# ==================== КЛАСС ДЛЯ УПРАВЛЕНИЯ ДАННЫМИ ПОЛЬЗОВАТЕЛЕЙ ====================

class UserDataManager:
    
    @staticmethod
    def get_user_dir(user_id):
        user_dir = USERS_DIR / str(user_id)
        user_dir.mkdir(exist_ok=True)
        return user_dir
    
    @staticmethod
    def get_user_games_file(user_id):
        return UserDataManager.get_user_dir(user_id) / 'games.json'
    
    @staticmethod
    def get_user_settings_file(user_id):
        return UserDataManager.get_user_dir(user_id) / 'settings.json'
    
    @staticmethod
    def get_user_history_file(user_id):
        return UserDataManager.get_user_dir(user_id) / 'history.json'
    
    @staticmethod
    def save_user_games(user_id, games):
        try:
            games_file = UserDataManager.get_user_games_file(user_id)
            with open(games_file, 'w', encoding='utf-8') as f:
                json.dump(games, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"Ошибка сохранения игр пользователя {user_id}: {e}")
            return False
    
    @staticmethod
    def load_user_games(user_id):
        try:
            games_file = UserDataManager.get_user_games_file(user_id)
            if games_file.exists():
                with open(games_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return []
        except Exception as e:
            print(f"Ошибка загрузки игр пользователя {user_id}: {e}")
            return []
    
    @staticmethod
    def save_user_settings(user_id, settings):
        try:
            settings_file = UserDataManager.get_user_settings_file(user_id)
            with open(settings_file, 'w', encoding='utf-8') as f:
                json.dump(settings, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"Ошибка сохранения настроек пользователя {user_id}: {e}")
            return False
    
    @staticmethod
    def load_user_settings(user_id):
        try:
            settings_file = UserDataManager.get_user_settings_file(user_id)
            if settings_file.exists():
                with open(settings_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {
                'notifications': True,
                'language': 'ru',
                'created_at': datetime.now().isoformat(),
                'last_active': datetime.now().isoformat()
            }
        except Exception as e:
            print(f"Ошибка загрузки настроек пользователя {user_id}: {e}")
            return {}
    
    @staticmethod
    def add_game_to_user(user_id, game_info):
        games = UserDataManager.load_user_games(user_id)
        
        for game in games:
            if game['game_id'] == game_info['game_id'] and game['store'] == game_info['store']:
                return False
        
        game_info['added_at'] = datetime.now().isoformat()
        game_info['last_check'] = datetime.now().isoformat()
        game_info['price_history'] = [{
            'date': datetime.now().isoformat(),
            'price': game_info['price'],
            'discount': game_info.get('discount', 0)
        }]
        
        games.append(game_info)
        UserDataManager.save_user_games(user_id, games)
        
        settings = UserDataManager.load_user_settings(user_id)
        settings['last_active'] = datetime.now().isoformat()
        settings['total_games'] = len(games)
        UserDataManager.save_user_settings(user_id, settings)
        
        return True
    
    @staticmethod
    def remove_game_from_user(user_id, game_id, store):
        games = UserDataManager.load_user_games(user_id)
        games = [g for g in games if not (g['game_id'] == game_id and g['store'] == store)]
        UserDataManager.save_user_games(user_id, games)
        
        settings = UserDataManager.load_user_settings(user_id)
        settings['last_active'] = datetime.now().isoformat()
        settings['total_games'] = len(games)
        UserDataManager.save_user_settings(user_id, settings)
        
        return True
    
    @staticmethod
    def get_user_game_by_id(user_id, game_id, store):
        games = UserDataManager.load_user_games(user_id)
        for game in games:
            if game['game_id'] == game_id and game['store'] == store:
                return game
        return None
    
    @staticmethod
    def update_user_game(user_id, game_info):
        games = UserDataManager.load_user_games(user_id)
        for i, game in enumerate(games):
            if game['game_id'] == game_info['game_id'] and game['store'] == game_info['store']:
                if 'price_history' not in game:
                    game['price_history'] = []
                
                game['price_history'].append({
                    'date': datetime.now().isoformat(),
                    'price': game_info['price'],
                    'discount': game_info.get('discount', 0)
                })
                
                if len(game['price_history']) > 50:
                    game['price_history'] = game['price_history'][-50:]
                
                game.update(game_info)
                game['last_check'] = datetime.now().isoformat()
                
                games[i] = game
                UserDataManager.save_user_games(user_id, games)
                return True
        return False
    
    @staticmethod
    def get_all_users():
        users = []
        for user_dir in USERS_DIR.iterdir():
            if user_dir.is_dir() and user_dir.name.isdigit():
                users.append(int(user_dir.name))
        return users
    
    @staticmethod
    def get_user_stats(user_id):
        games = UserDataManager.load_user_games(user_id)
        settings = UserDataManager.load_user_settings(user_id)
        
        stats = {
            'total_games': len(games),
            'by_store': {},
            'total_spent': 0,
            'total_saved': 0,
            'created_at': settings.get('created_at', 'Unknown'),
            'last_active': settings.get('last_active', 'Unknown')
        }
        
        for game in games:
            store = game['store']
            if store not in stats['by_store']:
                stats['by_store'][store] = 0
            stats['by_store'][store] += 1
            
            if len(game.get('price_history', [])) > 1:
                first_price = game['price_history'][0]['price']
                current_price = game['price']
                if current_price < first_price:
                    stats['total_saved'] += first_price - current_price
            
            stats['total_spent'] += game['price']
        
        return stats
    
    @staticmethod
    def add_notification_history(user_id, game_name, discount_info):
        try:
            history_file = UserDataManager.get_user_history_file(user_id)
            history = []
            
            if history_file.exists():
                with open(history_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
            
            history.append({
                'date': datetime.now().isoformat(),
                'game_name': game_name,
                'discount': discount_info,
                'read': False
            })
            
            if len(history) > 100:
                history = history[-100:]
            
            with open(history_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Ошибка сохранения истории уведомлений: {e}")

# ==================== УЛУЧШЕННЫЕ ПАРСЕРЫ ====================

class SteamParser:
    @staticmethod
    def parse(url):
        try:
            game_id = None
            if 'store.steampowered.com/app/' in url:
                game_id = url.split('/app/')[1].split('/')[0]
            elif 'steamcommunity.com' in url:
                game_id = re.search(r'/app/(\d+)', url)
                if game_id:
                    game_id = game_id.group(1)
            
            if not game_id:
                return None
            
            cache_file = CACHE_DIR / f'steam_{game_id}.json'
            if cache_file.exists():
                cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
                if cache_age.seconds < 3600:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
            
            api_url = f"https://store.steampowered.com/api/appdetails?appids={game_id}&cc=ru&l=russian"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
            }
            
            response = requests.get(api_url, headers=headers, timeout=10)
            data = response.json()
            
            if data[str(game_id)]['success']:
                game_data = data[str(game_id)]['data']
                name = game_data['name']
                
                price_data = {'price': 0, 'original_price': 0, 'discount': 0, 'currency': 'RUB'}
                
                if 'price_overview' in game_data:
                    price_overview = game_data['price_overview']
                    price_data['price'] = price_overview['final'] / 100
                    price_data['original_price'] = price_overview['initial'] / 100
                    price_data['discount'] = price_overview['discount_percent']
                    price_data['currency'] = price_overview.get('currency', 'RUB')
                
                image = game_data.get('header_image', '')
                if not image:
                    image = game_data.get('capsule_image', '')
                
                is_free = game_data.get('is_free', False)
                if is_free:
                    price_data['price'] = 0
                    price_data['original_price'] = 0
                    price_data['discount'] = 0
                
                game_info = {
                    'game_id': str(game_id),
                    'name': name,
                    'price': price_data['price'],
                    'original_price': price_data['original_price'],
                    'discount': price_data['discount'],
                    'currency': price_data['currency'],
                    'image': image,
                    'store': 'steam',
                    'url': f"https://store.steampowered.com/app/{game_id}",
                    'is_free': is_free,
                    'last_updated': datetime.now().isoformat()
                }
                
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(game_info, f, ensure_ascii=False, indent=2)
                
                return game_info
            
        except Exception as e:
            print(f"Ошибка парсинга Steam: {e}")
            
            cache_file = CACHE_DIR / f'steam_{game_id}.json'
            if cache_file.exists():
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        
        return None

class EpicGamesParser:
    @staticmethod
    def parse(url):
        try:
            if '/p/' not in url:
                return None
            
            game_slug = url.split('/p/')[1].split('?')[0].split('/')[0]
            
            cache_file = CACHE_DIR / f'epic_{game_slug}.json'
            if cache_file.exists():
                cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
                if cache_age.seconds < 3600:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
            
            game_info = EpicGamesParser._parse_api(game_slug)
            
            if game_info:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(game_info, f, ensure_ascii=False, indent=2)
            
            return game_info
            
        except Exception as e:
            print(f"Ошибка парсинга Epic Games: {e}")
            return None
    
    @staticmethod
    def _parse_api(game_slug):
        try:
            api_url = "https://store-site-backend-static.ak.epicgames.com/freeGamesPromotions"
            params = {
                'locale': 'ru',
                'country': 'RU',
                'allowCountries': 'RU'
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(api_url, params=params, headers=headers, timeout=10)
            data = response.json()
            
            for game in data.get('data', {}).get('Catalog', {}).get('searchStore', {}).get('elements', []):
                if game.get('productSlug') == game_slug:
                    return EpicGamesParser._parse_game_data(game)
            
            return None
        except:
            return None
    
    @staticmethod
    def _parse_game_data(game):
        try:
            name = game.get('title', 'Unknown')
            
            price = 0
            original_price = 0
            discount = 0
            
            promotions = game.get('promotions', {})
            if promotions:
                current_promos = promotions.get('promotionalOffers', [])
                for promo in current_promos:
                    for offer in promo.get('promotionalOffers', []):
                        if offer.get('discountSetting', {}).get('discountType') == 'PERCENTAGE':
                            discount = offer.get('discountSetting', {}).get('discountPercentage', 0)
                            if discount == 100:
                                price = 0
            
            if 'price' in game:
                price_info = game['price'].get('totalPrice', {})
                if price_info:
                    if price == 0:
                        original_price = price_info.get('originalPrice', 0) / 100
                    else:
                        price = price_info.get('discountPrice', 0) / 100
                        original_price = price_info.get('originalPrice', 0) / 100
                        if original_price > 0 and price > 0:
                            discount = int((original_price - price) / original_price * 100)
            
            image = ''
            if 'keyImages' in game and game['keyImages']:
                for img in game['keyImages']:
                    if img.get('type') in ['DieselGameBox', 'OfferImageWide']:
                        image = img.get('url', '')
                        break
            
            return {
                'game_id': game.get('id', game_slug),
                'name': name,
                'price': price,
                'original_price': original_price,
                'discount': discount,
                'currency': 'RUB',
                'image': image,
                'store': 'epic',
                'url': f"https://store.epicgames.com/ru/p/{game.get('productSlug', '')}",
                'is_free': price == 0 and original_price > 0,
                'last_updated': datetime.now().isoformat()
            }
        except:
            return None

class PlayStationParser:
    @staticmethod
    def parse(url):
        try:
            product_id = None
            patterns = [
                r'/product/([^/]+)',
                r'/-/([^/]+)',
                r'/games/([^/]+)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, url)
                if match:
                    product_id = match.group(1)
                    break
            
            if not product_id:
                product_id = url.split('/')[-1]
            
            cache_file = CACHE_DIR / f'ps_{product_id}.json'
            if cache_file.exists():
                cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
                if cache_age.seconds < 3600:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
            
            game_info = {
                'game_id': product_id,
                'name': f'PlayStation Game {product_id}',
                'price': 2999,
                'original_price': 2999,
                'discount': 0,
                'currency': 'RUB',
                'image': '',
                'store': 'playstation',
                'url': url,
                'is_free': False,
                'last_updated': datetime.now().isoformat()
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(game_info, f, ensure_ascii=False, indent=2)
            
            return game_info
        except Exception as e:
            print(f"Ошибка парсинга PlayStation: {e}")
            return None

class XboxParser:
    @staticmethod
    def parse(url):
        try:
            product_id = None
            patterns = [
                r'/product/([^/]+)',
                r'/p/([^/]+)',
                r'/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})',
                r'/([A-Z0-9]+)/?$'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, url, re.I)
                if match:
                    product_id = match.group(1)
                    break
            
            if not product_id:
                product_id = url.split('/')[-1]
            
            cache_file = CACHE_DIR / f'xbox_{product_id}.json'
            if cache_file.exists():
                cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
                if cache_age.seconds < 3600:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
            
            game_info = {
                'game_id': product_id,
                'name': f'Xbox Game {product_id}',
                'price': 2499,
                'original_price': 2499,
                'discount': 0,
                'currency': 'RUB',
                'image': '',
                'store': 'xbox',
                'url': url,
                'is_free': False,
                'last_updated': datetime.now().isoformat()
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(game_info, f, ensure_ascii=False, indent=2)
            
            return game_info
        except Exception as e:
            print(f"Ошибка парсинга Xbox: {e}")
            return None

# ==================== ОСНОВНЫЕ ФУНКЦИИ ====================

def get_game_info(url):
    url_lower = url.lower()
    
    if 'steampowered.com' in url_lower or 'steamcommunity.com' in url_lower:
        return SteamParser.parse(url)
    elif 'epicgames.com' in url_lower:
        return EpicGamesParser.parse(url)
    elif 'playstation.com' in url_lower or 'sony.com' in url_lower:
        return PlayStationParser.parse(url)
    elif 'xbox.com' in url_lower or 'microsoft.com' in url_lower:
        return XboxParser.parse(url)
    
    return None

def get_store_emoji(store):
    emojis = {
        'steam': '🟦 Steam',
        'epic': '🟪 Epic Games',
        'playstation': '🎮 PlayStation',
        'xbox': '🟢 Xbox'
    }
    return emojis.get(store, '🛒')

def get_free_games_text():
    return """
⚠️ **СРОЧНО!** ⚠️
🎁 **Бесплатные игры в Steam:**
👉 [Just Move:Clean City Messy Battle](https://store.steampowered.com/app/3414900/Just_MoveClean_City_Messy_Battle/) до 4.03 21:00
🔥 **Успей забрать!** 🔥
    """

# ==================== КОМАНДЫ БОТА ====================

@bot.message_handler(commands=['start'])
def send_welcome(message):
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("""INSERT OR IGNORE INTO users 
                 (user_id, username, first_name, last_activity) 
                 VALUES (?, ?, ?, ?)""",
              (user_id, username, first_name, datetime.now()))
    conn.commit()
    conn.close()
    
    settings = UserDataManager.load_user_settings(user_id)
    settings['username'] = username or first_name
    settings['first_name'] = first_name
    settings['first_seen'] = settings.get('created_at', datetime.now().isoformat())
    settings['last_active'] = datetime.now().isoformat()
    UserDataManager.save_user_settings(user_id, settings)
    
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton('➕ Добавить игру'),
        types.KeyboardButton('📋 Мои игры'),
        types.KeyboardButton('❌ Удалить игру'),
        types.KeyboardButton('📊 Моя статистика'),
        types.KeyboardButton('ℹ️ Помощь'),
        types.KeyboardButton('🛒 Магазины'),
        types.KeyboardButton('⏰ Время проверки'),
        types.KeyboardButton('🎁 Бесплатные игры')
    )
    
    welcome_text = f"""
👋 **Привет, {first_name}! Я бот для отслеживания скидок на игры!**

🆔 **Твой ID в системе:** `{user_id}`

🎮 **Поддерживаемые магазины:**
• 🟦 Steam
• 🟪 Epic Games Store
• 🎮 PlayStation Store
• 🟢 Xbox Store

**Как это работает:**
1️⃣ Отправь мне ссылку на игру
2️⃣ Я сохраню её в твоей личной папке
3️⃣ Буду проверять цены каждые 3 часа
4️⃣ При появлении скидки - сразу напишу!

✅ **Твои игры хранятся отдельно от других пользователей!**

{get_free_games_text()}
    """
    
    bot.send_message(message.chat.id, welcome_text, 
                    reply_markup=markup, 
                    parse_mode='Markdown',
                    disable_web_page_preview=True)

@bot.message_handler(func=lambda message: message.text == '🎁 Бесплатные игры')
def send_free_games(message):
    free_games_text = f"""
{get_free_games_text()}

🎮 **Другие магазины тоже часто дарят игры!**
Подпишись на уведомления и не пропусти:
• Бесплатные игры в Epic Games Store каждую неделю
• Раздачи в Steam
• Скидки в PlayStation и Xbox
    """
    bot.send_message(message.chat.id, free_games_text, 
                    parse_mode='Markdown',
                    disable_web_page_preview=True)

@bot.message_handler(func=lambda message: message.text == '📊 Моя статистика')
def show_user_stats(message):
    user_id = message.from_user.id
    stats = UserDataManager.get_user_stats(user_id)
    settings = UserDataManager.load_user_settings(user_id)
    
    stores_text = ""
    for store, count in stats['by_store'].items():
        emoji = get_store_emoji(store)
        stores_text += f"{emoji}: {count} игр\n"
    
    now = datetime.now()
    current_hour = now.hour
    next_check_hour = ((current_hour // 3) + 1) * 3
    
    if next_check_hour >= 24:
        next_check_hour = 0
        check_date = now.date() + timedelta(days=1)
    else:
        check_date = now.date()
    
    next_check = datetime.combine(check_date, datetime.min.time().replace(hour=next_check_hour))
    time_until = next_check - now
    hours = int(time_until.total_seconds() // 3600)
    minutes = int((time_until.total_seconds() % 3600) // 60)
    
    stats_text = f"""
📊 **Твоя статистика:**

👤 **Пользователь:** {settings.get('first_name', 'Unknown')} (@{settings.get('username', 'None')})
🆔 **ID:** `{user_id}`
📅 **Зарегистрирован:** {settings.get('created_at', 'Unknown')[:10]}
🕐 **Последняя активность:** {settings.get('last_active', 'Unknown')[:16]}

🎮 **Всего игр:** {stats['total_games']}
{stores_text}

💰 **Потрачено всего:** {stats['total_spent']:.2f} руб.
💸 **Сэкономлено:** {stats['total_saved']:.2f} руб.

⏰ **Следующая проверка:** через {hours} ч {minutes} мин

{get_free_games_text()}
    """
    
    bot.send_message(message.chat.id, stats_text, 
                    parse_mode='Markdown',
                    disable_web_page_preview=True)

@bot.message_handler(func=lambda message: message.text and (
    'http://' in message.text.lower() or 'https://' in message.text.lower()
))
def handle_game_url(message):
    user_id = message.from_user.id
    url = message.text.strip()
    
    status_msg = bot.send_message(message.chat.id, "🔍 Получаю информацию об игре...")
    
    game_info = get_game_info(url)
    
    if not game_info:
        bot.edit_message_text(
            "❌ **Не удалось получить информацию об игре**\n\n"
            "Проверьте правильность ссылки и поддерживается ли магазин\n\n"
            f"{get_free_games_text()}",
            message.chat.id,
            status_msg.message_id,
            parse_mode='Markdown',
            disable_web_page_preview=True
        )
        return
    
    if UserDataManager.add_game_to_user(user_id, game_info):
        store_emoji = get_store_emoji(game_info['store'])
        
        response = f"""✅ **Игра добавлена в отслеживание!**

{store_emoji} **{game_info['name']}**

💰 **Текущая цена:** {game_info['price']} {game_info.get('currency', 'руб.')}

📁 **ID пользователя:** `{user_id}`
⏳ Я буду проверять цену каждые 3 часа и уведомлю о скидках!

{get_free_games_text()}"""
        
        if game_info.get('image'):
            try:
                bot.delete_message(message.chat.id, status_msg.message_id)
                bot.send_photo(message.chat.id, game_info['image'], 
                             caption=response, 
                             parse_mode='Markdown',
                             disable_web_page_preview=True)
                return
            except:
                pass
        
        bot.edit_message_text(response, message.chat.id, status_msg.message_id, 
                            parse_mode='Markdown',
                            disable_web_page_preview=True)
    else:
        bot.edit_message_text(
            "⚠️ **Эта игра уже добавлена в отслеживание!**\n\n"
            f"{get_free_games_text()}",
            message.chat.id,
            status_msg.message_id,
            parse_mode='Markdown',
            disable_web_page_preview=True
        )

def show_my_games(message):
    user_id = message.from_user.id
    games = UserDataManager.load_user_games(user_id)
    
    if not games:
        bot.send_message(message.chat.id, 
                        "📭 **У вас пока нет отслеживаемых игр**\n\n"
                        "Отправьте ссылку на игру, чтобы начать!\n\n"
                        f"{get_free_games_text()}",
                        parse_mode='Markdown',
                        disable_web_page_preview=True)
        return
    
    stores = {}
    for game in games:
        store = game['store']
        if store not in stores:
            stores[store] = []
        stores[store].append(game)
    
    response = f"📋 **Ваши отслеживаемые игры:**\n\n"
    response += f"🆔 **ID:** `{user_id}`\n"
    response += f"⏰ **Проверка:** каждые 3 часа\n\n"
    
    for store, games_list in stores.items():
        store_emoji = get_store_emoji(store)
        response += f"{store_emoji}\n"
        
        for i, game in enumerate(games_list, 1):
            added = game.get('added_at', 'Unknown')[:10]
            response += f"  {i}. **{game['name'][:50]}**\n"
            response += f"     💰 {game['price']} руб. | 📅 {added}\n"
        
        response += "\n"
        
        if len(response) > 3500:
            bot.send_message(message.chat.id, response, 
                           parse_mode='Markdown',
                           disable_web_page_preview=True)
            response = ""
    
    response += f"\n{get_free_games_text()}"
    
    if response:
        bot.send_message(message.chat.id, response, 
                        parse_mode='Markdown',
                        disable_web_page_preview=True)

def show_delete_menu(message):
    user_id = message.from_user.id
    games = UserDataManager.load_user_games(user_id)
    
    if not games:
        bot.send_message(message.chat.id, 
                        "📭 **У вас нет игр для удаления**\n\n"
                        f"{get_free_games_text()}",
                        parse_mode='Markdown',
                        disable_web_page_preview=True)
        return
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    for game in games:
        store_emoji = get_store_emoji(game['store'])
        markup.add(types.InlineKeyboardButton(
            f"{store_emoji} {game['name'][:40]}...", 
            callback_data=f"delete_{game['game_id']}_{game['store']}"
        ))
    
    bot.send_message(message.chat.id, 
                    "❌ **Выберите игру для удаления:**\n\n"
                    f"🆔 ID: `{user_id}`\n\n"
                    f"{get_free_games_text()}",
                    reply_markup=markup,
                    parse_mode='Markdown',
                    disable_web_page_preview=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith('delete_'))
def delete_game(call):
    user_id = call.from_user.id
    _, game_id, store = call.data.split('_')
    
    game = UserDataManager.get_user_game_by_id(user_id, game_id, store)
    
    if game:
        game_name = game['name']
        UserDataManager.remove_game_from_user(user_id, game_id, store)
        
        bot.answer_callback_query(call.id, f"✅ Игра удалена")
        bot.edit_message_text(
            f"✅ **Игра успешно удалена из отслеживания!**\n\n"
            f"{game_name}\n\n"
            f"🆔 ID: `{user_id}`\n\n"
            f"{get_free_games_text()}",
            call.message.chat.id,
            call.message.message_id,
            parse_mode='Markdown',
            disable_web_page_preview=True
        )
    else:
        bot.answer_callback_query(call.id, "❌ Ошибка: игра не найдена")

def check_discounts_3hour():
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"🔄 Запуск проверки скидок в {current_time}")
    
    try:
        users = UserDataManager.get_all_users()
        print(f"👥 Найдено пользователей: {len(users)}")
        
        total_checks = 0
        total_discounts = 0
        
        for user_id in users:
            games = UserDataManager.load_user_games(user_id)
            settings = UserDataManager.load_user_settings(user_id)
            
            if not settings.get('notifications', True):
                continue
            
            for game in games:
                try:
                    print(f"  Проверяю: {game['name'][:30]}...")
                    
                    game_info = get_game_info(game['url'])
                    
                    if game_info:
                        current_price = game_info['price']
                        last_price = game['price']
                        
                        if current_price < last_price:
                            discount_percent = int((last_price - current_price) / last_price * 100)
                            print(f"  🎉 Найдена скидка {discount_percent}%!")
                            
                            send_discount_notification(user_id, game_info, last_price, discount_percent)
                            
                            UserDataManager.add_notification_history(
                                user_id, 
                                game['name'], 
                                f"{discount_percent}%"
                            )
                            
                            total_discounts += 1
                        
                        UserDataManager.update_user_game(user_id, game_info)
                        total_checks += 1
                    
                    time.sleep(random.uniform(2, 5))
                    
                except Exception as e:
                    print(f"  ❌ Ошибка: {e}")
                    continue
            
            settings['last_active'] = datetime.now().isoformat()
            UserDataManager.save_user_settings(user_id, settings)
        
        print(f"✅ Проверка завершена в {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Проверено: {total_checks} игр")
        print(f"   Найдено скидок: {total_discounts}")
        
        log_file = LOGS_DIR / f'check_{datetime.now().strftime("%Y%m%d")}.log'
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()}: Проверено {total_checks} игр, найдено {total_discounts} скидок\n")
        
    except Exception as e:
        print(f"❌ Ошибка при проверке: {e}")

def send_discount_notification(user_id, game_info, old_price, discount_percent):
    try:
        store_emoji = get_store_emoji(game_info['store'])
        
        message = f"""🎉 **НОВАЯ СКИДКА!** 🎉

{store_emoji} **{game_info['name']}**

🔥 **Скидка:** {discount_percent}%
💰 **Старая цена:** {old_price} {game_info.get('currency', 'руб.')}
💵 **Новая цена:** {game_info['price']} {game_info.get('currency', 'руб.')}

🔗 [Перейти в магазин]({game_info['url']})

🆔 **ID пользователя:** `{user_id}`

{get_free_games_text()}"""
        
        if game_info.get('image'):
            try:
                bot.send_photo(user_id, game_info['image'], 
                             caption=message, 
                             parse_mode='Markdown',
                             disable_web_page_preview=True)
            except:
                bot.send_message(user_id, message, 
                               parse_mode='Markdown',
                               disable_web_page_preview=True)
        else:
            bot.send_message(user_id, message, 
                           parse_mode='Markdown',
                           disable_web_page_preview=True)
            
    except Exception as e:
        print(f"Не удалось отправить уведомление: {e}")

def schedule_3hour_check():
    while True:
        now = datetime.now()
        current_hour = now.hour
        next_check_hour = ((current_hour // 3) + 1) * 3
        
        if next_check_hour >= 24:
            next_check_hour = 0
            check_date = now.date() + timedelta(days=1)
        else:
            check_date = now.date()
        
        next_check = datetime.combine(check_date, datetime.min.time().replace(hour=next_check_hour))
        wait_seconds = (next_check - now).total_seconds()
        
        print(f"⏰ Следующая проверка в {next_check.strftime('%Y-%m-%d %H:%M')}")
        print(f"⏳ Ожидание {wait_seconds/3600:.1f} часов")
        
        time.sleep(max(1, wait_seconds))
        check_discounts_3hour()

def send_help(message):
    user_id = message.from_user.id
    help_text = f"""
📚 **Как пользоваться ботом:**

1️⃣ **Добавить игру**: 
   • Нажми кнопку "➕ Добавить игру"
   • Или просто отправь ссылку на игру

2️⃣ **Посмотреть список**: 
   Нажми "📋 Мои игры"

3️⃣ **Удалить игру**: 
   Нажми "❌ Удалить игру"

4️⃣ **Статистика**: 
   Нажми "📊 Моя статистика"

5️⃣ **Бесплатные игры**: 
   Нажми "🎁 Бесплатные игры"

🆔 **Твой ID в системе:** `{user_id}`

🌐 **Поддерживаемые магазины:**
• 🟦 Steam
• 🟪 Epic Games Store
• 🎮 PlayStation Store
• 🟢 Xbox Store

⏰ **Проверка:** каждые 3 часа (00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00)

📁 **Где хранятся данные:** в папке `users/{user_id}/` на сервере

{get_free_games_text()}
    """
    bot.send_message(message.chat.id, help_text, 
                    parse_mode='Markdown',
                    disable_web_page_preview=True)

def show_stores_info(message):
    stores_text = f"""
🛒 **Поддерживаемые магазины:**

🟦 **Steam**
• URL: [store.steampowered.com](https://store.steampowered.com)
• Регионы: все
• Особенности: официальное API, регулярные распродажи

🟪 **Epic Games Store**
• URL: [store.epicgames.com](https://store.epicgames.com)
• Регионы: РФ, СНГ
• Особенности: бесплатные игры каждую неделю

🎮 **PlayStation Store**
• URL: [store.playstation.com](https://store.playstation.com)
• Регионы: РФ, EU, US
• Особенности: эксклюзивы PlayStation

🟢 **Xbox Store**
• URL: [xbox.com](https://www.xbox.com)
• Регионы: все
• Особенности: Game Pass интеграция

📌 **Как добавить игру:**
Просто отправьте ссылку на игру из любого магазина!

{get_free_games_text()}
    """
    bot.send_message(message.chat.id, stores_text, 
                    parse_mode='Markdown',
                    disable_web_page_preview=True)

def show_check_time(message):
    now = datetime.now()
    current_hour = now.hour
    next_check_hour = ((current_hour // 3) + 1) * 3
    
    if next_check_hour >= 24:
        next_check_hour = 0
        check_date = now.date() + timedelta(days=1)
    else:
        check_date = now.date()
    
    next_check = datetime.combine(check_date, datetime.min.time().replace(hour=next_check_hour))
    time_until = next_check - now
    hours = int(time_until.total_seconds() // 3600)
    minutes = int((time_until.total_seconds() % 3600) // 60)
    
    check_times = ["00:00", "03:00", "06:00", "09:00", "12:00", "15:00", "18:00", "21:00"]
    times_str = ", ".join(check_times)
    
    time_text = f"""
⏰ **Информация о проверке скидок:**

• **Расписание:** Каждые 3 часа
• **Время:** {times_str} (МСК)
• **Следующая проверка:** {next_check.strftime('%H:%M')}
• **Осталось:** {hours} ч {minutes} мин

📊 **Всего проверок сегодня:** считаем в реальном времени
📁 **Данные пользователей:** папка `users/`

{get_free_games_text()}
    """
    bot.send_message(message.chat.id, time_text, 
                    parse_mode='Markdown',
                    disable_web_page_preview=True)

@bot.message_handler(func=lambda message: message.text in ['➕ Добавить игру', '📋 Мои игры', '❌ Удалить игру', 'ℹ️ Помощь', '🛒 Магазины', '⏰ Время проверки', '📊 Моя статистика', '🎁 Бесплатные игры'])
def handle_buttons(message):
    if message.text == '➕ Добавить игру':
        bot.send_message(message.chat.id, 
                        "📤 **Отправь мне ссылку на игру**\n\n"
                        "Примеры:\n"
                        "• `https://store.steampowered.com/app/730/`\n"
                        "• `https://store.epicgames.com/ru/p/cyberpunk-2077`\n"
                        "• `https://store.playstation.com/ru-ru/product/EP0001-CUSA12345_00-GAME000000000000`\n"
                        "• `https://www.xbox.com/ru-ru/games/store/cyberpunk-2077/9MWH6ZB0ZR7N`\n\n"
                        f"{get_free_games_text()}",
                        parse_mode='Markdown',
                        disable_web_page_preview=True)
    
    elif message.text == '📋 Мои игры':
        show_my_games(message)
    
    elif message.text == '❌ Удалить игру':
        show_delete_menu(message)
    
    elif message.text == '📊 Моя статистика':
        show_user_stats(message)
    
    elif message.text == 'ℹ️ Помощь':
        send_help(message)
    
    elif message.text == '🛒 Магазины':
        show_stores_info(message)
    
    elif message.text == '⏰ Время проверки':
        show_check_time(message)
    
    elif message.text == '🎁 Бесплатные игры':
        send_free_games(message)

# ==================== КОМАНДЫ АДМИНИСТРАТОРА ====================

@bot.message_handler(commands=['all_users'])
def show_all_users(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "⛔ Недостаточно прав")
        return
    
    users = UserDataManager.get_all_users()
    
    text = "📊 **Все пользователи:**\n\n"
    for user_id in users:
        settings = UserDataManager.load_user_settings(user_id)
        games = UserDataManager.load_user_games(user_id)
        text += f"• `{user_id}` - {settings.get('first_name', 'Unknown')} (@{settings.get('username', 'None')}) - {len(games)} игр\n"
    
    bot.send_message(message.chat.id, text, 
                    parse_mode='Markdown',
                    disable_web_page_preview=True)

@bot.message_handler(commands=['check_now'])
def force_check(message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "⛔ Недостаточно прав")
        return
    
    bot.send_message(message.chat.id, "🔄 Запускаю внеочередную проверку...")
    thread = threading.Thread(target=check_discounts_3hour)
    thread.start()

# ==================== ЗАПУСК БОТА ====================

if __name__ == '__main__':
    print("""
╔════════════════════════════════════════════════════════════╗
║         🎮 Game Discount Bot - Запуск                      ║
╠════════════════════════════════════════════════════════════╣
║  • Бот работает в фоновом режиме                           ║
║  • Проверка каждые 3 часа                                  ║
║  • Используйте Ctrl+C для выхода                           ║
╚════════════════════════════════════════════════════════════╝
    """)
    
    print("🚀 Инициализация базы данных и файловой структуры...")
    init_database()
    
    print(f"📁 Структура папок создана:")
    print(f"   • {USERS_DIR} - данные пользователей")
    print(f"   • {CACHE_DIR} - кэш игр")
    print(f"   • {LOGS_DIR} - логи")
    print(f"   • {ERRORS_DIR} - ошибки")
    print(f"   • {BACKUP_DIR} - резервные копии")
    
    print(f"⏰ Запуск планировщика проверок...")
    scheduler_thread = threading.Thread(target=schedule_3hour_check, daemon=True)
    scheduler_thread.start()
    
    print("✅ Бот запущен!")
    print(f"👤 Администраторы: {len(ADMIN_IDS)}")
    print(f"📊 Команда /all_users доступна только администраторам")
    print(f"🎁 Добавлена кнопка 'Бесплатные игры'")
    
    try:
        bot.infinity_polling()
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен пользователем")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")