import psycopg2
import csv
import datetime
import os

from dotenv import load_dotenv
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def ensure_schema():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS instruments (
        instrument_id SERIAL PRIMARY KEY,
        symbol VARCHAR(1024) UNIQUE NOT NULL,
        instrument_type VARCHAR(32),
        option_type VARCHAR(8),
        strike_price NUMERIC,
        expiry_date DATE,
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS candle_data_1min (
        time TIMESTAMP WITH TIME ZONE NOT NULL,
        instrument_id INTEGER REFERENCES instruments(instrument_id),
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT,
        open_interest BIGINT,
        PRIMARY KEY (time, instrument_id)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS technical_indicators (
        time TIMESTAMP WITH TIME ZONE NOT NULL,
        instrument_id INTEGER REFERENCES instruments(instrument_id),
        timeframe VARCHAR(16) NOT NULL,
        tvi NUMERIC, obv NUMERIC, rsi NUMERIC, pvi NUMERIC, pvt NUMERIC,
        PRIMARY KEY (time, instrument_id, timeframe)
    );
    """)
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    cur.execute("SELECT create_hypertable('candle_data_1min', 'time', if_not_exists => TRUE);")
    cur.execute("SELECT create_hypertable('candle_data_15sec', 'time', if_not_exists => TRUE);")
    cur.execute("SELECT create_hypertable('technical_indicators', 'time', if_not_exists => TRUE, migrate_data => TRUE);")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS candle_data_15sec (
        time TIMESTAMP WITH TIME ZONE NOT NULL,
        instrument_id INTEGER REFERENCES instruments(instrument_id),
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT,
        open_interest BIGINT,
        PRIMARY KEY (time, instrument_id)
    );
    """)
    # PATCH: Add trading_signals table for pipeline validation
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trading_signals (
        signal_id SERIAL PRIMARY KEY,
        time TIMESTAMP WITH TIME ZONE NOT NULL,
        instrument_id INTEGER REFERENCES instruments(instrument_id),
        signal_type VARCHAR(32),
        signal_value NUMERIC,
        extra JSONB,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    """)
    # PATCH: Add first_two_hours_data view for validation (adjust logic as needed)
    cur.execute("""
    CREATE OR REPLACE VIEW first_two_hours_data AS
    SELECT cd.*
    FROM candle_data_1min cd
    JOIN instruments i ON cd.instrument_id = i.instrument_id
    WHERE EXTRACT(HOUR FROM cd.time AT TIME ZONE 'UTC') BETWEEN 9 AND 10
    ORDER BY cd.time ASC;
    """)
    # --- FIX: Drop scalping_data view before re-creating ---
    cur.execute('DROP VIEW IF EXISTS scalping_data CASCADE;')
    cur.execute("""
    CREATE VIEW scalping_data AS
    SELECT 
        cd."time",
        i.symbol,
        i.instrument_type,
        i.option_type,
        i.strike_price,
        cd.open,
        cd.high,
        cd.low,
        cd.close,
        cd.volume,
        cd.open_interest
    FROM candle_data_1min cd
    JOIN instruments i ON cd.instrument_id = i.instrument_id
    ORDER BY cd."time" DESC;
    """)
    # (Retention policy code removed)

    conn.commit()
    cur.close()
    conn.close()

def import_historical_csv(csv_path="historical_data.csv", candle_type="1min"):
    assert candle_type in ("1min", "15sec")
    table = "candle_data_1min" if candle_type == "1min" else "candle_data_15sec"
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()

    def get_or_create_instrument_id(symbol):
        cursor.execute("SELECT instrument_id FROM instruments WHERE symbol=%s", (symbol,))
        result = cursor.fetchone()
        if result:
            return result[0]
        # PATCH: Add support for more types if needed, fallback logic
        instrument_type = "option" if "CE" in symbol or "PE" in symbol else "index"
        cursor.execute(
            "INSERT INTO instruments (symbol, instrument_type) VALUES (%s, %s) RETURNING instrument_id",
            (symbol, instrument_type)
        )
        return cursor.fetchone()[0]

    with open(csv_path, "r", newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            symbol = row["Ticker"]
            instrument_id = get_or_create_instrument_id(symbol)
            date_str = str(row["Date"])
            time_str = str(row["Time"]).zfill(4)
            # PATCH: Always store as UTC and timezone-aware
            dt = datetime.datetime.strptime(date_str + time_str, "%Y%m%d%H%M")
            dt = dt.replace(tzinfo=datetime.timezone.utc)
            open_, high, low, close = float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"])
            volume = int(float(row["Volume"])) if row["Volume"] else 0
            oi = int(float(row["OI"])) if row["OI"] else None
            cursor.execute(f"""
                INSERT INTO {table}
                    (time, instrument_id, open, high, low, close, volume, open_interest)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time, instrument_id) DO NOTHING
            """, (dt, instrument_id, open_, high, low, close, volume, oi))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Historical data imported from {csv_path} to {table}!")