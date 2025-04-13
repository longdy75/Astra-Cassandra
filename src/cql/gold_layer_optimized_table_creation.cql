-- Dylan Long - 4.11.25

-- Gold layer denormalization of data 
-- Tables optimized for queries

-- Region specific queries
  CREATE TABLE IF NOT EXISTS sales.sales_by_region_date(
    region TEXT,
    order_date DATE,
    order_id INT,
    country TEXT,
    item_type TEXT,
    sales_channel TEXT,
    order_priority TEXT,
    units_sold INT,
    total_revenue FLOAT,
    total_cost FLOAT,
    total_profit FLOAT,
    PRIMARY KEY ((region, order_date), order_id)
    )

-- Country specific queries
  CREATE TABLE IF NOT EXISTS sales.sales_by_country(
    region TEXT,
    country TEXT,
    order_date DATE,
    order_id INT,
    item_type TEXT,
    sales_channel TEXT,
    order_priority TEXT,
    units_sold INT,
    total_revenue DECIMAL,
    total_cost DECIMAL,
    total_profit DECIMAL,
    PRIMARY KEY ((region, country), order_date, order_id)
    )


-- Order specific queries
  CREATE TABLE IF NOT EXISTS sales.order_details(
    order_id INT,
    region TEXT,
    country TEXT,
    item_type TEXT,
    sales_channel TEXT,
    order_priority TEXT,
    order_date DATE,
    ship_date DATE,
    units_sold INT,
    unit_price DECIMAL,
    unit_cost DECIMAL,
    total_revenue DECIMAL,
    total_cost DECIMAL,
    total_profit DECIMAL,
    PRIMARY KEY (order_id)
    )


-- Item specific queries
  CREATE TABLE IF NOT EXISTS sales.sales_by_item_type (
    item_type TEXT,
    order_date DATE,
    order_id INT,
    region TEXT,
    country TEXT,
    sales_channel TEXT,
    order_priority TEXT,
    units_sold INT,
    total_revenue DECIMAL,
    total_cost DECIMAL,
    total_profit DECIMAL,
    PRIMARY KEY ((item_type), order_date, order_id)
    )