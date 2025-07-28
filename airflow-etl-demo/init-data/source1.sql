CREATE TABLE customers (
  id SERIAL PRIMARY KEY,
  name TEXT,
  email TEXT
);

INSERT INTO customers (name, email)
VALUES ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com');
