-- Loads TPC-H .tbl files into PostgreSQL tables using COPY
-- Adjust path as needed or run from the directory containing .tbl files

\COPY nation FROM 'nation.tbl' WITH (FORMAT csv, DELIMITER '|');
\COPY region FROM 'region.tbl' WITH (FORMAT csv, DELIMITER '|');
\COPY part FROM 'part.tbl' WITH (FORMAT csv, DELIMITER '|');
\COPY supplier FROM 'supplier.tbl' WITH (FORMAT csv, DELIMITER '|');
\COPY partsupp FROM 'partsupp.tbl' WITH (FORMAT csv, DELIMITER '|');
\COPY customer FROM 'customer.tbl' WITH (FORMAT csv, DELIMITER '|');
\COPY orders FROM 'orders.tbl' WITH (FORMAT csv, DELIMITER '|');
\COPY lineitem FROM 'lineitem.tbl' WITH (FORMAT csv, DELIMITER '|');
