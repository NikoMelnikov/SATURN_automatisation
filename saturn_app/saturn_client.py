import requests
import pandas as pd
import os
from datetime import datetime

URL = "https://api.fgis-saturn.ru/probeInnerArm/innerArm/seapi/"

HEADERS = {
    'Content-Type': 'application/json',
}


def get_contractor_by_inn(token, inn, output_dir=None):
    """
    Найти контрагента по ИНН.
    Возвращает (success: bool, message: str, data: dict|None, filepath: str|None)
    """
    headers = HEADERS.copy()
    headers['Authorization'] = f"Bearer {token}"
    
    payload = {
        "com": "execOperation",
        "op": "static/getList()",
        "otype": "Contractor",
        "opargs": {
            "pos": 0,
            "size": 10,
            "getFullCards": 1,
            "filters": [
                {
                    "column": "inn",
                    "condition": "=",
                    "value": [str(inn)]
                },
                {
                    "column": "lcState",
                    "condition": "=",
                    "value": ["actual"]
                }
            ]
        }
    }
    
    try:
        response = requests.post(URL, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 401:
            return False, "Введите актуальный токен", None, None
        
        if response.status_code != 200:
            return False, f"Ошибка API: {response.status_code}", None, None
        
        registr_data = response.json()
        
        if 'resData' not in registr_data:
            return False, "Некорректный ответ API", None, None
        
        obj_array = registr_data['resData']['objList']['_OBJ_ARRAY']
        
        if not obj_array:
            return False, "Компания не найдена или не зарегистрирована во ФГИС САТУРН", None, None
        
        obj = obj_array[0]
        
        data = {
            'ID': obj.get('id'),
            'Наименование': obj.get('name'),
            'ИНН': obj.get('INN'),
            'Статус': obj.get('lcState'),
            'Руководитель': obj.get('responsiblePerson'),
            'Юридический адрес': obj.get('legalAddress')
        }
        
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(__file__), 'exports')
        
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'Контрагент_{inn}_{timestamp}.xlsx'
        filepath = os.path.join(output_dir, filename)
        
        df = pd.DataFrame([data])
        df.to_excel(filepath, index=False)
        
        return True, "Контрагент найден", data, filepath
        
    except requests.exceptions.Timeout:
        return False, "Превышено время ожидания ответа", None, None
    except requests.exceptions.RequestException as e:
        return False, f"Ошибка соединения: {str(e)}", None, None
    except Exception as e:
        return False, f"Ошибка: {str(e)}", None, None


def get_invoices_on_way(token, output_dir=None):
    """
    Выгрузить накладные в статусе "В пути".
    Возвращает (success: bool, message: str, filepath: str|None)
    """
    headers = HEADERS.copy()
    headers['Authorization'] = f"Bearer {token}"
    
    payload = {
        "com": "execOperation",
        "op": "static/getList()",
        "otype": "Invoice",
        "opargs": {
            "filters": [
                {
                    "column": "lcState",
                    "condition": "=",
                    "value": ["onWay"]
                },
                {
                    "column": "recieverContractorId",
                    "condition": "in",
                    "value": [248824]
                }
            ],
            "size": 200,
            "getFullCards": 0
        }
    }
    
    try:
        response = requests.post(URL, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 401:
            return False, "Введите актуальный токен", None
        
        if response.status_code != 200:
            return False, f"Ошибка API: {response.status_code}", None
        
        invoice_data = response.json()
        
        if 'resData' not in invoice_data:
            return False, "Некорректный ответ API", None
        
        attr_table = invoice_data['resData']['attrTable']
        
        if not attr_table or len(attr_table) < 2:
            return False, "Нет накладных в статусе 'В пути'", None
        
        df = pd.DataFrame(attr_table[1:], columns=attr_table[0])
        
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(__file__), 'exports')
        
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'Накладные_В_пути_{timestamp}.xlsx'
        filepath = os.path.join(output_dir, filename)
        
        df.to_excel(filepath, index=False)
        
        return True, f"Выгружено {len(df)} накладных", filepath
        
    except requests.exceptions.Timeout:
        return False, "Превышено время ожидания ответа", None
    except requests.exceptions.RequestException as e:
        return False, f"Ошибка соединения: {str(e)}", None
    except Exception as e:
        return False, f"Ошибка: {str(e)}", None


def get_warehouses(token, output_dir=None):
    """
    Выгрузить список складов в Excel.
    Возвращает (success: bool, message: str, filepath: str|None)
    """
    headers = HEADERS.copy()
    headers['Authorization'] = f"Bearer {token}"
    
    payload = {
        "otype": "Warehouse",
        "com": "execOperation",
        "op": "static/getList()",
        "opargs": {
            "pos": 0,
            "size": 1500,
            "getFullCards": 1,
            "filters": [
                {
                    "column": "ownerId",
                    "condition": "=",
                    "value": ["248824"]
                }
            ]
        }
    }
    
    try:
        response = requests.post(URL, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 401:
            return False, "Введите актуальный токен", None
        
        if response.status_code != 200:
            return False, f"Ошибка API: {response.status_code}", None
        
        warehouse_data = response.json()
        
        if 'resData' not in warehouse_data:
            return False, "Некорректный ответ API", None
        
        objects_list = warehouse_data['resData']['objList']['_OBJ_ARRAY']
        
        if not objects_list:
            return False, "Список складов пуст", None
        
        data_for_df = []
        for obj in objects_list:
            data_for_df.append({
                'название': obj.get('name'),
                'адрес': obj.get('address'),
                'статус': obj.get('lcState'),
                'последнее редактирование': obj.get('uco_areaLastEdDate'),
                'номер объекта': obj.get('uco_objectNumber'),
                'регион': obj.get('subName'),
                'id объекта': obj.get('id'),
                'не зарегистрирован': obj.get('isUnregKeepersWarehouse'),
                'временное хранение': obj.get('isTempStoringArea'),
            })
        
        df = pd.DataFrame(data_for_df)
        
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(__file__), 'exports')
        
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'Список_складов_{timestamp}.xlsx'
        filepath = os.path.join(output_dir, filename)
        
        df.to_excel(filepath, index=False)
        
        return True, f"Выгружено {len(df)} записей", filepath
        
    except requests.exceptions.Timeout:
        return False, "Превышено время ожидания ответа", None
    except requests.exceptions.RequestException as e:
        return False, f"Ошибка соединения: {str(e)}", None
    except Exception as e:
        return False, f"Ошибка: {str(e)}", None
