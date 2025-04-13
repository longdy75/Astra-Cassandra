# Dylan Long - 4.11.25

from python.db_config import get_cassandra_session
import pandas as pd

# Setup session
session = get_cassandra_session()

# Extract portion of ELT
try:
    df = pd.read_csv('sales_100.csv')
    print("CSV loaded successfully")
except Exception as e:
    print(f"Error loading CSV: {e}")
    raise


# Prepare INSERT statement for the table
insert_raw = session.prepare("""
    INSERT INTO sales.raw_sales_data (
        region, country, item_type, sales_channel, order_priority,
        order_date, order_id, ship_date, units_sold, unit_price,
        unit_cost, total_revenue, total_cost, total_profit
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Insert raw data as string to ingest everything successfully
for index, row in df.iterrows():
    try:
        session.execute(insert_raw, (
            str(row['Region']),
            str(row['Country']),
            str(row['Item Type']),
            str(row['Sales Channel']),
            str(row['Order Priority']),
            str(row['Order Date']),
            str(row['Order ID']),
            str(row['Ship Date']),
            str(row['UnitsSold']),
            str(row['UnitPrice']),
            str(row['UnitCost']),
            str(row['TotalRevenue']),
            str(row['TotalCost']),
            str(row['TotalProfit'])
        ))
    except Exception as e:
        print(f"Error inserting row with Order ID {row['Order ID']}: {e}")

print("Raw data loaded successfully.")

#cluster.shutdown()