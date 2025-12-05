import os
import json
import requests
import pandas as pd
from time import sleep
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional


env_file = Path("../.env")
print(".env exists:", env_file.exists(), "→", env_file)

# Загружаем .env
loaded = load_dotenv(dotenv_path=env_file)
# Загружаем переменные
URL = os.getenv('URL')
HEADERS = {
    'Content-Type': os.getenv('CONTENT_TYPE'),
    'Authorization': os.getenv('AUTHORIZATION'),
}


def get_warehouse_stocks(url: str, headers: Dict) -> Optional[pd.DataFrame]:
    """
    Получает текущие остатки на складе
    """
    try:
        current_time = datetime.now(timezone.utc).isoformat()
        
        payload = {
            "otype": "WarehouseStates",
            "com": "execOperation",
            "op": "static/getTotals()",
            "opargs": {
                "theCard": {
                    "dateTime": current_time,
                    "groupBy": "pat"
                }
            }
        }
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        goods_data = response.json()
        
        if "resData" not in goods_data or "rows" not in goods_data["resData"]:
            print("Нет данных об остатках в ответе API")
            return None
            
        df_wr = pd.DataFrame(goods_data["resData"]["rows"])
        print(f"Загружено {len(df_wr)} записей об остатках")
        return df_wr
        
    except Exception as e:
        print(f"Ошибка при получении остатков: {e}")
        return None


class InvoiceCounter:
    """Класс для управления счетчиком номеров накладных для установки номера документа"""
    _counter = 1
    
    @classmethod
    def get_next(cls) -> str:
        date_str = datetime.now().strftime('%Y%m%d')
        doc_num = f"{date_str}-{cls._counter:03d}"
        cls._counter += 1
        return doc_num
    
    @classmethod
    def reset(cls):
        cls._counter = 1


def create_invoice_payload(items: List[Dict], row: pd.Series) -> Dict:
    """Создает payload для создания накладной в статусе черновик"""
    current_time = datetime.now(timezone.utc).isoformat()
    doc_num = InvoiceCounter.get_next()
    
    return {
        "com": "execOperation",
        "oid": "0",
        "op": "static/createNew()",
        "otype": "Invoice",
        "opargs": {
            "theCard": {
                "head": {
                    "docDate": current_time,
                    "docNote": f"Invoice_to_retail_{len(items)}_items",
                    "docNum": doc_num,
                    "name": f"Подготовка для списания в розницу ({len(items)} позиций)",
                    "receiverContractorId": int(row['contractorId']),
                    "sourceWarehouseId": int(row['warehouseId'])
                },
                "tbrDtoList": [
                    {
                        "batchId": int(item['batchId']),
                        "countPuSent": item['countPuSent']
                    } for item in items
                ]
            }
        }
    }


def create_retail_payload(invoice_id: int) -> Dict:
    """Создает payload для отправки накладной в розницу"""
    return {
        "com": "execOperation",
        "oid": str(invoice_id),
        "op": "draft/doSendToRetale()",
        "otype": "Invoice",
        "opargs": {}
    }


def prepare_invoices(df: pd.DataFrame, items_per_invoice: int, max_invoices: Optional[int]) -> List[Dict]:
    """
    Формирует накладные на основе принципа FIFO (по validFrom),
    фильтрует zero-count и записи со словом 'списание со склада'
    """
    # Проверяем наличие необходимых колонок
    required_columns = ['countPu', 'batchId', 'validFrom', 'contractorId', 'warehouseId']
    for col in required_columns:
        if col not in df.columns:
            print(f"Отсутствует колонка {col}")
    
    # Фильтрация
    df_filtered = df.copy()
    
    # Конвертируем countPu в число
    df_filtered['countPu'] = pd.to_numeric(df_filtered['countPu'], errors='coerce')
    
    # Фильтруем чтобы не подхватить партии с нулевым остатком
    df_filtered = df_filtered[df_filtered['countPu'] > 0]
    
    # Фильтруем записи со 'списание со склада' в note, этого правда не достаточно
    if 'note' in df_filtered.columns:
        df_filtered = df_filtered[
            ~df_filtered['note'].astype(str).str.contains('списание со склада', case=False, na=False)
        ]
    
    if df_filtered.empty:
        print("Нет данных для формирования накладных после фильтрации")
        return []
    
    print(f"После фильтрации осталось {len(df_filtered)} записей")
    
    # Подготовка типов
    df_filtered['validFrom'] = pd.to_datetime(df_filtered['validFrom'], utc=True, errors='coerce')
    
    # Сортировка FIFO
    df_sorted = df_filtered.sort_values(['validFrom', 'batchId']).reset_index(drop=True)
    
    # Группировка по партиям (с сохранением всех необходимых данных)
    # Убираем колонки, которые будут использоваться для группировки
    group_columns = ['batchId', 'patId', 'warehouseId', 'contractorId']
    
    # Создаем словарь агрегации без колонок группировки
    agg_dict = {
        'countPu': 'sum',
        'validFrom': 'min',
        'warehouseName': 'first',
        'contractorName': 'first',
        'patName': 'first'
    }
    
    # Оставляем только существующие колонки
    existing_columns = {k: v for k, v in agg_dict.items() if k in df_sorted.columns}
    
    # Группируем по batchId, patId и warehouseId
    batch_groups = df_sorted.groupby(group_columns).agg(existing_columns).reset_index()
    
    print(f"Сгруппировано в {len(batch_groups)} партий")
    
    # Формируем накладные
    invoices = []
    current_items = []
    
    for _, row in batch_groups.iterrows():
        remaining_quantity = row['countPu']
        
        # Обрабатываем всю партию
        while remaining_quantity > 0:
            # Сколько можно добавить в текущую накладную
            available_space = items_per_invoice - len(current_items)
            
            if available_space > 0:
                # Добавляем всю партию
                current_items.append({
                    'batchId': row['batchId'],
                    'countPuSent': float(remaining_quantity)
                })
                
                # Если накладная заполнилась
                if len(current_items) >= items_per_invoice:
                    invoices.append(create_invoice_payload(current_items, row))
                    current_items = []
                
                remaining_quantity = 0  # Вся партия обработана
            else:
                # Текущая накладная заполнена, создаем новую
                if current_items:
                    invoices.append(create_invoice_payload(current_items, row))
                    current_items = []
    
    # Добавляем последнюю неполную накладную
    if current_items:
        invoices.append(create_invoice_payload(current_items, batch_groups.iloc[-1]))
    
    # Ограничиваем количество накладных
    if max_invoices:
        invoices = invoices[:max_invoices]
    
    print(f"Сформировано {len(invoices)} накладных")
    return invoices


def run_retail_write_off_service(
    df: pd.DataFrame,
    url: str,
    headers: Dict,
    items_per_invoice: int,
    max_invoices: Optional[int],
    execute: bool
) -> Tuple[List[Dict], List[Dict]]:
    """
    Единый сервис: подготавливает накладные по FIFO,
    создаёт их в системе и сразу отправляет в розницу.
    """
    # Сбрасываем счетчик при каждом запуске
    InvoiceCounter.reset()
    
    # 1. Формирование накладных
    print("\n" + "="*60)
    print("ФОРМИРОВАНИЕ НАКЛАДНЫХ")
    print("="*60)
    
    invoices = prepare_invoices(df, items_per_invoice, max_invoices)
    
    if not invoices:
        print("Нет накладных для обработки")
        return [], []
    
    print(f"\n✓ Сформировано {len(invoices)} накладных:")
    for i, invoice in enumerate(invoices, 1):
        doc_num = invoice['opargs']['theCard']['head']['docNum']
        items_count = len(invoice['opargs']['theCard']['tbrDtoList'])
        warehouse_id = invoice['opargs']['theCard']['head']['sourceWarehouseId']
        contractor_id = invoice['opargs']['theCard']['head']['receiverContractorId']
        print(f"  {i:2d}. {doc_num}: {items_count:2d} позиций, "
              f"склад: {warehouse_id}, контрагент: {contractor_id}")
    
    results = []
    if not execute:
        print("\n" + "="*60)
        print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
        print(f"Было бы создано {len(invoices)} накладных")
        print("Для реальной отправки устанавливаем --execute")
        print("="*60)
        return invoices, results
    
    # 2. Создание и отправка
    print("\n" + "="*60)
    print("ОТПРАВКА В САТУРН")
    print("="*60)
    
    for idx, inv_payload in enumerate(invoices, start=1):
        doc_num = inv_payload['opargs']['theCard']['head']['docNum']
        
        try:
            # Создаем накладную в статусе черновик
            print(f"\n[{idx}/{len(invoices)}] Создание накладной {doc_num}...")
            print(f"   Позиций: {len(inv_payload['opargs']['theCard']['tbrDtoList'])}")
            
            resp = requests.post(url, headers=headers, json=inv_payload, timeout=30)
            resp.raise_for_status()
            
            draft = resp.json()
            
            # Проверяем структуру ответа
            if 'resData' not in draft or 'id' not in draft['resData']:
                print(f"   ✗ Неверный формат ответа API")
                results.append({
                    'docNum': doc_num,
                    'error': 'Invalid API response format',
                    'status': 'error'
                })
                continue
                
            invoice_id = draft['resData']['id']
            print(f"   ✓ Черновик создан, ID: {invoice_id}")
            
            # Отправляем в розницу
            print(f"   Отправка в розницу...")
            retail_payload = create_retail_payload(invoice_id)
            retail_resp = requests.post(url, headers=headers, json=retail_payload, timeout=30)
            retail_resp.raise_for_status()
            
            print(f"   ✓ Успешно отправлена в розницу")
            
            results.append({
                'docNum': doc_num,
                'invoice_id': invoice_id,
                'items_count': len(inv_payload['opargs']['theCard']['tbrDtoList']),
                'status': 'created_and_sent',
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
            
            # Пауза между запросами
            if idx < len(invoices):
                sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            error_msg = f"HTTP error: {e}"
            print(f"   ✗ Ошибка сети: {e}")
            results.append({
                'docNum': doc_num,
                'error': error_msg,
                'status': 'network_error',
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            print(f"   ✗ Ошибка при обработке: {e}")
            results.append({
                'docNum': doc_num,
                'error': str(e),
                'status': 'error',
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
    
    # Выводим итоги
    print("\n" + "="*60)
    print("ИТОГИ ОБРАБОТКИ:")
    print("="*60)
    
    successful = len([r for r in results if r['status'] == 'created_and_sent'])
    failed = len(results) - successful
    
    print(f"Успешно обработано: {successful}")
    print(f"Не удалось обработать: {failed}")
    
    if successful > 0:
        print("\nУспешные накладные:")
        for result in results:
            if result['status'] == 'created_and_sent':
                print(f"  ✓ {result['docNum']} (ID: {result['invoice_id']})")
    
    if failed > 0:
        print("\nОшибки:")
        for result in results:
            if result['status'] != 'created_and_sent':
                print(f"  ✗ {result['docNum']}: {result.get('error', 'Unknown error')}")
    
    print("="*60)
    
    return invoices, results


def main():
    """
    Основная функция запуска сервиса с аргументами командной строки
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Сервис автоматического списания товаров в розницу',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s                         # Тестовый прогон (2 накладные по 2 позиции)
  %(prog)s --execute               # Реальная отправка (2 накладные по 2 позиции)
  %(prog)s --execute --limit 10    # Отправка максимум 10 накладных
  %(prog)s --execute --items 5     # По 5 позиций в накладной
  %(prog)s --execute --no-limit    # Отправка всех накладных без ограничений
        """
    )
    
    parser.add_argument('--execute', action='store_true', 
                       help='Реальная отправка (по умолчанию - тестовый режим)')
    parser.add_argument('--limit', type=int, default=2,
                       help='Максимальное количество накладных (по умолчанию: 2)')
    parser.add_argument('--items', type=int, default=2,
                       help='Максимальное позиций в накладной (по умолчанию: 2)')
    parser.add_argument('--no-limit', action='store_true',
                       help='Без ограничения количества накладных (переопределяет --limit)')
    
    args = parser.parse_args()
    
    print("Запуск сервиса списания в розницу")
    print("=" * 60)
    
    # Проверяем конфигурацию
    if not URL or not HEADERS.get('Authorization'):
        print("Ошибка: не настроены переменные окружения")
        print(f"URL: {'Установлен' if URL else 'НЕ установлен'}")
        print(f"Authorization: {'Установлен' if HEADERS.get('Authorization') else 'НЕ установлен'}")
        return
    
    # Получаем данные со склада
    print("Получение данных об остатках со склада...")
    df = get_warehouse_stocks(URL, HEADERS)
    
    if df is None:
        print("Не удалось получить данные от API")
        return
    
    if df.empty:
        print("Нет данных об остатках для обработки")
        return
    
    print(f"Получено {len(df)} записей об остатках")
    
    # Определяем лимит накладных
    if args.no_limit:
        max_invoices = None
        print("Режим: БЕЗ ОГРАНИЧЕНИЯ количества накладных")
    else:
        max_invoices = args.limit
        print(f"Лимит накладных: {max_invoices}")
    
    print(f"Позиций в накладной: {args.items}")
    print(f"Режим отправки: {'РЕАЛЬНАЯ ОТПРАВКА' if args.execute else 'ТЕСТИРОВАНИЕ'}")
    print("=" * 60)
    
    # Запускаем сервис
    try:
        invoices, results = run_retail_write_off_service(
            df=df,
            url=URL,
            headers=HEADERS,
            items_per_invoice=args.items,
            max_invoices=max_invoices,
            execute=args.execute
        )
        
        # Сохраняем результаты
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            if invoices:
                invoices_file = f"invoices_{timestamp}.json"
                with open(invoices_file, 'w', encoding='utf-8') as f:
                    json.dump(invoices, f, ensure_ascii=False, indent=2)
                print(f"\n✓ Сформированные накладные сохранены в {invoices_file}")
            
            if results:
                results_file = f"results_{timestamp}.json"
                with open(results_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                print(f"✓ Результаты обработки сохранены в {results_file}")
            
            print(f"\nЛоги сохранены с префиксом: {timestamp}")
                
        except Exception as e:
            print(f"\n⚠ Не удалось сохранить файлы: {e}")
            
    except KeyboardInterrupt:
        print("\n\nПрервано пользователем")
    except Exception as e:
        print(f"\nОшибка при выполнении сервиса: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()