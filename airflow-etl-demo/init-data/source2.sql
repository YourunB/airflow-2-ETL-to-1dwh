CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  customer_id INT,
  amount NUMERIC
);

INSERT INTO orders (customer_id, amount)
VALUES (1, 99.99), (2, 149.50);
