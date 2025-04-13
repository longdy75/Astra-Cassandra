# Dylan Long - 4.11.25

from python.db_config import get_cassandra_session
from decimal import Decimal
import pandas as pd

# Setup session
session = get_cassandra_session()

# Fetch typed_data and put in data frame
rows = session.execute("SELECT * FROM sales.typed_data")
df = pd.DataFrame([row._asdict() for row in rows])

# Data Cleaning
# Drop rows with missing critical fields
critical_columns = ['order_id', 'region', 'country', 'item_type', 'sales_channel', 'order_priority', 'order_date', 'ship_date']
df = df.dropna(subset=critical_columns)

# Fill missing numerical fields with imputed mean
numerical_columns = ['units_sold', 'unit_price', 'unit_cost', 'total_revenue', 'total_cost', 'total_profit']
for col in numerical_columns:
    df[col] = df[col].fillna(df[col].mean())

# Drop orders with total_cost = 0
df = df[df['total_cost'] != 0]

# Remove duplicates based on order_id
df = df.drop_duplicates(subset=['order_id'])

# Standardize text fields
text_columns = ['region', 'country', 'item_type', 'sales_channel', 'order_priority']
for col in text_columns:
    df[col] = df[col].str.strip().str.lower()


session.execute("TRUNCATE sales.typed_data")

# Prepare the INSERT statement
insert_cleaned = session.prepare("""
    INSERT INTO sales.typed_data (
        order_id, region, country, item_type, sales_channel, order_priority,
        order_date, ship_date, units_sold, unit_price, unit_cost,
        total_revenue, total_cost, total_profit
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Insert cleaned data back into typed_data
for index, row in df.iterrows():
    try:
        session.execute(insert_cleaned, (
            int(row['order_id']),
            row['region'],
            row['country'],
            row['item_type'],
            row['sales_channel'],
            row['order_priority'],
            row['order_date'],
            row['ship_date'],
            int(row['units_sold']),
            Decimal(str(row['unit_price'])),
            Decimal(str(row['unit_cost'])),
            Decimal(str(row['total_revenue'])),
            Decimal(str(row['total_cost'])),
            Decimal(str(row['total_profit']))
        ))
    except Exception as e:
        print(f"Error inserting row with Order ID {row['order_id']}: {e}")

print("Data cleaned and reinserted successfully.")

# Close the connection
# cluster.shutdown()