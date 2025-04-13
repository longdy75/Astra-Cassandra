# Dylan Long - 4.11.25

from python.db_config import get_cassandra_session
from datetime import datetime

# Setup session
session = get_cassandra_session()


# Fetch raw data from table
rows = session.execute("SELECT * FROM sales.raw_sales_data")

# Prepare INSERT statement
insert_typed = session.prepare("""
    INSERT INTO sales.typed_data (
        order_id, region, country, item_type, sales_channel, order_priority,
        order_date, ship_date, units_sold, unit_price, unit_cost,
        total_revenue, total_cost, total_profit
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Cast each row to intended dtype and change date format
for row in rows:
    try:
        order_date = datetime.strptime(row.order_date, '%m/%d/%Y').strftime('%Y-%m-%d')
        ship_date = datetime.strptime(row.ship_date, '%m/%d/%Y').strftime('%Y-%m-%d')
        
        session.execute(insert_typed, (
            int(row.order_id),
            row.region,
            row.country,
            row.item_type,
            row.sales_channel,
            row.order_priority,
            order_date,
            ship_date,
            int(row.units_sold),
            round(float(row.unit_price), 2),
            round(float(row.unit_cost), 2),
            round(float(row.total_revenue), 2),
            round(float(row.total_cost), 2),
            round(float(row.total_profit), 2)
        ))
    except Exception as e:
        print(f"Error transforming row with Order ID {row.order_id}: {e}")

print("Transformation complete.")
