
import os
import json
import time
import logging
import requests
import pandas as pd
from time import sleep
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timezone, timedelta

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

CONTRACTOR_ID = int(os.getenv("CONTRACTOR_ID", 248824))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", 200))
LOG_FILE = os.getenv("LOG_FILE", "invoice_service.log")

# --- Логирование ---
def setup_logging():
    logger = logging.getLogger("InvoiceService")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)-5s: %(message)s", "%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger

# --- Сервис обработки ---
class InvoiceService:
    def __init__(self, url, headers, contractor_id, page_size, logger):
        self.url = url
        self.headers = headers
        self.contractor_id = contractor_id
        self.page_size = page_size
        self.logger = logger

        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0

    # Загружаем накладные находящиеся в пути в наш адрес

    def fetch_invoices(self):
        payload = json.dumps({
            "com": "execOperation",
            "op": "static/getList()",
            "otype": "Invoice",
            "opargs": {
                "filters": [
                    {"column": "lcState", "condition": "=", "value": ["onWay"]},
                    {"column": "recieverContractorId", "condition": "in", "value": [self.contractor_id]}
                ],
                "size": self.page_size,
                "getFullCards": 0
            }
        })
        resp = requests.post(self.url, headers=self.headers, data=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        table = data["resData"]["attrTable"]
        df = pd.DataFrame(table[1:], columns=table[0])
        self.logger.info(f"Загружено накладных: {len(df)}")
        #print(df)
        return df
        

    def notify_delivered(self, invoice_id, destination_warehouse_id):
        current_time = datetime.now(timezone.utc).isoformat()
        payload = json.dumps({
            "com": "execOperation",
            "op": "onWay/notifyDelivered()",
            "oid": str(invoice_id),
            "otype": "Invoice",
            "opargs": {
                "theCard": {
                    "dateAction": current_time,
                    "description": "auto_notify",
                    "destinationWarehouseId": int(destination_warehouse_id)
                }
            }
        })
        
        resp = requests.post(self.url, headers=self.headers, data=payload, timeout=30)
        print(">>> BODY SENT:", resp.request.body)
        return resp

    def run(self):
        df = self.fetch_invoices()
        total = len(df)
        for idx, row in df.iterrows():
            invoice_id = row.get("id")
            dest_wh = row.get("destinationWarehouseId")
            # На случай если значение id склада пропущено или равно 0, можно развить два сценария: ошибка или значение по дефолту.
            '''
            if dest_wh == 0 or pd.isna(invoice_id) or pd.isna(dest_wh):
                self.logger.warning(f"#{idx+1}/{total} Пропущена запись ID={invoice_id}, склад={dest_wh}")
                self.skipped_count += 1
                continue
            '''
            if  pd.isna(invoice_id) or pd.isna(dest_wh) or dest_wh in (0, '0') :
                dest_wh = 1085300
                self.logger.warning(f"#{idx+1}/{total} Пропущена запись ID={invoice_id}, установлен склад={dest_wh}")
            try:
                resp = self.notify_delivered(invoice_id, dest_wh)
                status = resp.status_code
                self.logger.info(f"#{idx+1}/{total} ID={invoice_id} склад={dest_wh} => {status}")
                if status == 200:
                    self.success_count += 1
                else:
                    self.error_count += 1
                    self.logger.error(f"Ошибка HTTP {status}: {resp.text}")
            except Exception as exc:
                self.error_count += 1
                self.logger.error(f"Исключение для ID={invoice_id}: {exc}")

            sleep(0.5)

        # Итоговая статистика
        self.logger.info("=== Завершено ===")
        self.logger.info(f"Успешно: {self.success_count}")
        self.logger.info(f"Ошибок:   {self.error_count}")
        self.logger.info(f"Пропущено:{self.skipped_count}")

def main():
    logger = setup_logging()
    svc = InvoiceService(URL, HEADERS, CONTRACTOR_ID, PAGE_SIZE, logger)
    try:
        svc.run()
    except Exception as e:
        logger.exception(f"Сервис остановлен с ошибкой: {e}")

if __name__ == "__main__":
    main()