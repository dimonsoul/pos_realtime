import os
import time
import random
import pandas as pd
from datetime import datetime, timedelta

# Task 1: Advanced Workload & Edge Case Generation
# จำลอง POS transaction stream พร้อม edge case ที่ระบบจริงต้องเจอ:
#   1) Flash Sales      -> ยอดขายพุ่ง 10 เท่าในบางรอบ
#   2) Network Latency  -> ข้อมูลบางส่วนมาถึงช้ากว่าเวลาจริง (late-arriving data)
#   3) Data Quality     -> Transaction ID ซ้ำ และ Total Amount ติดลบ

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.environ.get("POS_OUTPUT_DIR", os.path.join(SCRIPT_DIR, "tmp", "pos_data"))

SIMULATION_BATCHES = 12       # จำลองทั้งหมด 12 รอบ (ครอบคลุม 1 ชม. โดยแบ่งรอบละ 5 นาที)
INTERVAL_SECONDS = 5          # พ่นไฟล์ออกมารับแรงกระแทกทุกๆ 5 วินาที
BASE_RECORDS_MIN = 15
BASE_RECORDS_MAX = 35

# --- Edge case configuration ---
FLASH_SALE_BATCHES = {4, 8}          # รอบที่จะเกิด Flash Sale (ยอดพุ่ง 10 เท่า)
FLASH_SALE_MULTIPLIER = 10
LATE_ARRIVAL_PROBABILITY = 0.15      # โอกาสที่แต่ละ record จะเป็นข้อมูลมาช้า (Late-Arriving Data)
LATE_ARRIVAL_MIN_MINUTES = 6
LATE_ARRIVAL_MAX_MINUTES = 45
DUPLICATE_ID_PROBABILITY = 0.05      # โอกาสที่ transaction_id จะซ้ำกับที่เคยออกไปแล้ว
NEGATIVE_AMOUNT_PROBABILITY = 0.03   # โอกาสที่ total_amount จะติดลบ (ข้อมูลขยะ/บั๊กจากเครื่อง POS)

STORES = ['POS_001', 'POS_002', 'POS_003']
PRODUCTS = [
    {'id': 'P101', 'name': 'Espresso', 'price': 55.0},
    {'id': 'P102', 'name': 'Latte', 'price': 65.0},
    {'id': 'P103', 'name': 'Croissant', 'price': 45.0},
    {'id': 'P104', 'name': 'Sandwich', 'price': 89.0}
]

_seen_transaction_ids = []  # เก็บ pool ของ transaction_id ที่เคยออกไปแล้ว เพื่อจำลอง duplicate


def _new_transaction_id():
    tx_id = f"TX-{random.randint(100000, 999999)}"
    _seen_transaction_ids.append(tx_id)
    return tx_id


def _pick_transaction_id():
    """คืนค่า transaction_id ปกติ หรือ id ซ้ำ (duplicate) ตามความน่าจะเป็นที่กำหนด"""
    if _seen_transaction_ids and random.random() < DUPLICATE_ID_PROBABILITY:
        return random.choice(_seen_transaction_ids[-100:])
    return _new_transaction_id()


def _pick_event_time(batch_time):
    """คืนค่า timestamp ปกติ หรือ timestamp ที่ 'มาช้า' (late-arriving) เพื่อจำลอง Network Latency"""
    if random.random() < LATE_ARRIVAL_PROBABILITY:
        delay = timedelta(minutes=random.randint(LATE_ARRIVAL_MIN_MINUTES, LATE_ARRIVAL_MAX_MINUTES))
        return batch_time - delay, True
    return batch_time + timedelta(seconds=random.randint(0, 299)), False


def generate_pos_batch(batch_num, batch_time, base_records=25):
    is_flash_sale = batch_num in FLASH_SALE_BATCHES
    num_records = base_records * FLASH_SALE_MULTIPLIER if is_flash_sale else random.randint(BASE_RECORDS_MIN, BASE_RECORDS_MAX)

    records = []
    stats = {'late_arrivals': 0, 'duplicates': 0, 'negative_amounts': 0}

    for _ in range(num_records):
        tx_id = _pick_transaction_id()
        if tx_id in [r['transaction_id'] for r in records]:
            stats['duplicates'] += 1

        event_time, is_late = _pick_event_time(batch_time)
        if is_late:
            stats['late_arrivals'] += 1

        item = random.choice(PRODUCTS)
        qty = random.randint(1, 4)
        total_amount = round(qty * item['price'], 2)

        if random.random() < NEGATIVE_AMOUNT_PROBABILITY:
            total_amount = -abs(total_amount)
            stats['negative_amounts'] += 1

        records.append({
            'transaction_id': tx_id,
            'store_id': random.choice(STORES),
            'timestamp': event_time.strftime('%Y-%m-%d %H:%M:%S'),
            'product_id': item['id'],
            'product_name': item['name'],
            'quantity': qty,
            'unit_price': item['price'],
            'total_amount': total_amount
        })

    return pd.DataFrame(records), is_flash_sale, stats


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"🚀 เริ่มการจำลอง POS Workload (พร้อม Edge Cases) สู่ {OUTPUT_DIR}...")
    sim_start_time = datetime.now() - timedelta(hours=1)

    for i in range(1, SIMULATION_BATCHES + 1):
        df_batch, is_flash_sale, stats = generate_pos_batch(i, sim_start_time)
        file_path = os.path.join(OUTPUT_DIR, f"pos_data_batch_{i}.csv")

        # เขียนลงไฟล์ชั่วคราวแล้วเปลี่ยนชื่อ เพื่อป้องกัน Sensor จับไฟล์ที่ยังเขียนไม่เสร็จ
        temp_path = f"{file_path}.tmp"
        df_batch.to_csv(temp_path, index=False)
        os.rename(temp_path, file_path)

        tags = []
        if is_flash_sale:
            tags.append(f"🔥 FLASH SALE x{FLASH_SALE_MULTIPLIER}")
        if stats['late_arrivals']:
            tags.append(f"🕒 late-arrivals={stats['late_arrivals']}")
        if stats['duplicates']:
            tags.append(f"♻️ duplicates={stats['duplicates']}")
        if stats['negative_amounts']:
            tags.append(f"⚠️ negative_amounts={stats['negative_amounts']}")
        tag_str = f" [{', '.join(tags)}]" if tags else ""

        print(f"[{datetime.now().strftime('%H:%M:%S')}] สร้างไฟล์: {file_path} (ข้อมูล {len(df_batch)} แถว){tag_str}")

        sim_start_time += timedelta(minutes=5)
        time.sleep(INTERVAL_SECONDS)

    print("✅ จำลองการส่งข้อมูลครบถ้วน (รวม Flash Sale / Late Data / Duplicate ID / Negative Amount)")
