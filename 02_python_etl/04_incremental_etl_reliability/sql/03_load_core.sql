INSERT INTO core.orders (
    order_id,
    line_id,
    order_date,
    customer_id,
    product_id,
    quantity,
    unit_price
)
SELECT
    order_id,
    line_id,
    order_date,
    customer_id,
    product_id,
    quantity,
    unit_price
FROM staging.orders;