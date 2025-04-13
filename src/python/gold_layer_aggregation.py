# Dylan Long - 4-12-25

from python.db_config import get_cassandra_session
import pandas as pd
from decimal import Decimal

# Setup session
session = get_cassandra_session()

rows = session.execute("SELECT * FROM sales.typed_data")
df = pd.DataFrame([row._asdict() for row in rows])

# Convert date to a usable pandas format
df['order_date'] = df['order_date'].apply(lambda x: x.date())

# Average Profit by Item Type
item_type_agg = df.groupby('item_type').agg({
    'total_profit': 'mean',
    'units_sold': 'sum'
}).reset_index()

# Prepare INSERT statement
insert_item_type = session.prepare("""
    INSERT INTO sales.avg_profit_by_item_type (
        item_type, avg_profit, total_units_sold
    ) VALUES (?, ?, ?)
""")

# Insert aggregated data
for index, row in item_type_agg.iterrows():
    try:
        session.execute(insert_item_type, (
            row['item_type'],
            Decimal(str(row['total_profit'])).quantize(Decimal('0.01')),
            int(row['units_sold'])
        ))
    except Exception as e:
        print(f"Error inserting item type aggregation: {e}")
        
        
# Total Revenue and Profit by Region and Month
# Extract year and month from order_date
df['month'] = pd.to_datetime(df['order_date']).dt.to_period('M').astype(str)

# Group by region and month
region_month_agg = df.groupby(['region', 'month']).agg({
    'total_revenue': 'sum',
    'total_profit': 'sum',
    'units_sold': 'sum'
}).reset_index()

# Prepare INSERT statement
insert_region_month = session.prepare("""
    INSERT INTO sales.revenue_profit_by_region_month (
        region, month, total_revenue, total_profit, total_units_sold
    ) VALUES (?, ?, ?, ?, ?)
""")

# Insert aggregated data
for index, row in region_month_agg.iterrows():
    try:
        session.execute(insert_region_month, (
            row['region'],
            row['month'],
            Decimal(str(row['total_revenue'])).quantize(Decimal('0.01')),
            Decimal(str(row['total_profit'])).quantize(Decimal('0.01')),
            int(row['units_sold'])
        ))
    except Exception as e:
        print(f"Error inserting aggregation: {e}")