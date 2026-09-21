CREATE TABLE IF NOT EXISTS core.orders (
    order_id VARCHAR(20) NOT NULL,
    line_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    customer_id VARCHAR(20),
    product_id VARCHAR(20) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,

    PRIMARY KEY (order_id, line_id),

    CHECK (quantity > 0),
    CHECK (unit_price >= 0)
);