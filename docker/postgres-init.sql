-- Demo schema for docker-compose: one table, published for curntValv.
CREATE TABLE orders (
    id   serial PRIMARY KEY,
    item text NOT NULL,
    qty  int  NOT NULL
);

CREATE PUBLICATION curntvalv FOR TABLE orders;
