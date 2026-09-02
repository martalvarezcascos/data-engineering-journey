/* Exercises to test knowledge from DataCamp Joining Data in SQL.
Data:

customers-------------------------------- 
customer_id 
customer_name 
country 
signup_date 

orders-------------------------------- 
order_id 
customer_id 
order_date 
status 

order_items-------------------------------- 
order_id 
product_id 
quantity 
unit_price

*/


/* 1. Return orders with client information */

SELECT o.order_id, o.order_date, c.customer_name, c.country
FROM customers c
INNER JOIN orders o
ON c.customer_id = o.customer_id;

/* 2. Return all clients and if they are made an order or not */

SELECT c.customer_id, c.customer_name, c.country, o.order_id, o.order_date
FROM customers c
LEFT JOIN orders o
ON c.customer_id = o.customer_id;

/* 3. Total cost of each order per customer */

SELECT o.order_id, o.order_date, c.customer_name, SUM(oi.quantity*oi.unit_price) AS total_order_amount
FROM customers c
INNER JOIN orders o
ON c.customer_id = o.customer_id
INNER JOIN order_items oi
ON o.order_id = oi.order_id
GROUP BY o.order_id, o.order_date, c.customer_name;

/* 4. Clients that have never made a purchase */

SELECT c.customer_id, c.customer_name
FROM customers c
LEFT JOIN orders o
ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;

/* 5. Trick question 
If we have customer 1 -> order 100 -> 3 rows in order_items
And do
customers
JOIN orders
JOIN order_items
And afterwards execute COUNT(customer_id)
How many clients do we count? */

-- Answer: Supposing we are joining based on the correct elements of each table, default JOIN in SQL is an INNER JOIN, so for customer 1 in order 100 if we have 3 rows in order_items, we will have 3 counts of the same customer_id. In order to just count one customer we should do COUNT(DISTINCT customer_id)

/* 6. Revenue by country */

SELECT 	c.country, 
	COUNT( DISTINCT c.customer_id) AS number_of_customers, 
	COUNT( DISTINCT o.order_id) AS number_of_orders,
	SUM( oi.quantity * oi.unit_price) AS total_revenue
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
INNER JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.country
ORDER BY total_revenue DESC;