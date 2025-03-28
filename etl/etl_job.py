import psycopg2
import pandas as pd
import time


# Step 1: Extract data from PostgreSQL
def extract_data():
    try:
        print("⏳ Waiting for PostgreSQL to be ready...")
        time.sleep(5)  # wait 5 seconds before connecting

        conn = psycopg2.connect(
            host="172.22.0.1",
            database="datapipeline",
            user="myuser",
            password="mypassword",
            port=5432
        )

        print("✅ Connected to PostgreSQL.")

    except Exception as e:
        print("❌ Connection failed:", e)
        return pd.DataFrame()  # return empty to avoid crash

    df = pd.read_sql("SELECT * FROM transactions", conn)
    conn.close()
    return df

# Step 2: Transform the data
def transform_data(df):
    # Group by customer_id and sum the amount
    agg_df = df.groupby('customer_id')['amount'].sum().reset_index()
    agg_df.rename(columns={'amount': 'total_spent'}, inplace=True)
    return agg_df

# Step 3: Load transformed data into a new table
def load_data(df):
    conn = psycopg2.connect(
        host="172.22.0.1",
        database="datapipeline",
        user="myuser",
        password="mypassword",
        port=5432
    )
    cur = conn.cursor()

    # Creating the table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_totals (
            customer_id INT PRIMARY KEY,
            total_spent NUMERIC
        )
    """)
    conn.commit()

    # Inserting or updating values
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO customer_totals (customer_id, total_spent)
            VALUES (%s, %s)
            ON CONFLICT (customer_id) DO UPDATE
            SET total_spent = EXCLUDED.total_spent
        """, (int(row['customer_id']), float(row['total_spent'])))

    conn.commit()
    cur.close()
    conn.close()

# Running the ETL job
if __name__ == "__main__":
    print("Running ETL job...")
    raw_data = extract_data()
    if raw_data.empty:
        print("No data found in 'transactions' table.")
    else:
        transformed = transform_data(raw_data)
        load_data(transformed)
        print("ETL job completed and customer_totals table updated.")
