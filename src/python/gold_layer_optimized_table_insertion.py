# Dylan Long - 4.12.25
from python.db_config import get_cassandra_session

session = get_cassandra_session()

rows = session.execute("SELECT * FROM sales.typed_data")

# Insert statements for each table
insert_region_date = session.prepare("""
    INSERT INTO sales.sales_by_region_date (
        region, order_date, order_id, country, item_type, sales_channel,
        order_priority, units_sold, total_revenue, total_cost, total_profit
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

insert_country = session.prepare("""
    INSERT INTO sales.sales_by_country (
        region, country, order_date, order_id, item_type, sales_channel,
        order_priority, units_sold, total_revenue, total_cost, total_profit
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

insert_order_details = session.prepare("""
    INSERT INTO sales.order_details (
        order_id, region, country, item_type, sales_channel, order_priority,
        order_date, ship_date, units_sold, unit_price, unit_cost,
        total_revenue, total_cost, total_profit
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

insert_item_type = session.prepare("""
    INSERT INTO sales.sales_by_item_type (
        item_type, order_date, order_id, region, country, sales_channel,
        order_priority, units_sold, total_revenue, total_cost, total_profit
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Insert data into tables
for index, row in df.iterrows():
    try:
        # Define variables 
        order_id = int(row['order_id'])
        region = row['region']
        country = row['country']
        item_type = row['item_type']
        sales_channel = row['sales_channel']
        order_priority = row['order_priority']
        order_date = row['order_date']
        ship_date = row['ship_date']
        units_sold = int(row['units_sold'])
        unit_price = row['unit_price']
        unit_cost = row['unit_cost']
        total_revenue = row['total_revenue']
        total_cost = row['total_cost']
        total_profit = row['total_profit']

        # Insert into sales_by_region_date
        session.execute(insert_region_date, (
            region,
            order_date,
            order_id,
            country,
            item_type,
            sales_channel,
            order_priority,
            units_sold,
            total_revenue,
            total_cost,
            total_profit
        ))

        # Insert into sales_by_country
        session.execute(insert_country, (
            region,
            country,
            order_date,
            order_id,
            item_type,
            sales_channel,
            order_priority,
            units_sold,
            total_revenue,
            total_cost,
            total_profit
        ))

        # Insert into order_details
        session.execute(insert_order_details, (
            order_id,
            region,
            country,
            item_type,
            sales_channel,
            order_priority,
            order_date,
            ship_date,
            units_sold,
            unit_price,
            unit_cost,
            total_revenue,
            total_cost,
            total_profit
        ))

        # Insert into sales_by_item_type
        session.execute(insert_item_type, (
            item_type,
            order_date,
            order_id,
            region,
            country,
            sales_channel,
            order_priority,
            units_sold,
            total_revenue,
            total_cost,
            total_profit
        ))

    except Exception as e:
        print(f"Error inserting row with Order ID {row['Order ID']}: {e}")

print("Data inserted successfully.")