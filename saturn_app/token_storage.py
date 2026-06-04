import os
import json

TOKEN_FILE = os.path.join(os.path.dirname(__file__), '.token.json')


def load_token():
    """Загрузить токен из файла"""
    if not os.path.exists(TOKEN_FILE):
        return None
    try:
        with open(TOKEN_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('token')
    except (json.JSONDecodeError, IOError):
        return None


def save_token(token):
    """Сохранить токен в файл"""
    try:
        with open(TOKEN_FILE, 'w', encoding='utf-8') as f:
            json.dump({'token': token}, f, indent=2)
        return True
    except IOError:
        return False


def delete_token():
    """Удалить файл токена"""
    try:
        if os.path.exists(TOKEN_FILE):
            os.remove(TOKEN_FILE)
        return True
    except IOError:
        return False
