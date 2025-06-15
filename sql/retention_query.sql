select * from retention_per_month;

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE';

