"""
run once to generate sample_sensor_data.csv
uses sir's exact hashing method
python generate_data.py
"""
import hashlib
import csv
import random
import os

SECRET_KEY = "sda_spring_2026_secure_key"
ITERATIONS = 100000
NUM_ROWS   = 200


def generate_signature(raw_value_str: str, key: str, iterations: int) -> str:
    """sir's exact function - key is password, raw_value is salt"""
    password_bytes = key.encode('utf-8')
    salt_bytes     = raw_value_str.encode('utf-8')
    hash_bytes     = hashlib.pbkdf2_hmac(
        hash_name  = 'sha256',
        password   = password_bytes,
        salt       = salt_bytes,
        iterations = iterations
    )
    return hash_bytes.hex()


os.makedirs('data', exist_ok=True)

rows    = []
sensors = ['SENSOR_A', 'SENSOR_B', 'SENSOR_C', 'SENSOR_D']

for i in range(NUM_ROWS):
    sensor_id     = random.choice(sensors)
    timestamp     = 1000 + i
    raw_value     = round(random.uniform(20.0, 80.0), 2)
    raw_value_str = f"{raw_value}"

    if random.random() < 0.8:
        signature = generate_signature(raw_value_str, SECRET_KEY, ITERATIONS)
    else:
        signature = "fake_signature_" + str(i)

    rows.append({
        'Sensor_ID':      sensor_id,
        'Timestamp':      timestamp,
        'Raw_Value':      raw_value,
        'Auth_Signature': signature
    })

with open('data/sample_sensor_data.csv', 'w', newline='') as f:
    writer = csv.DictWriter(
        f, fieldnames=['Sensor_ID','Timestamp','Raw_Value','Auth_Signature']
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"generated {NUM_ROWS} rows in data/sample_sensor_data.csv")
print(f"real packets ~{int(NUM_ROWS * 0.8)}")
print(f"fake packets ~{int(NUM_ROWS * 0.2)}")