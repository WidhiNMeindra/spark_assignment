CREATE STREAM nasabah_stream (
    nasabah_id STRING,
    nama STRING,
    alamat STRING,
    saldo INT,
    umur INT,
    pekerjaan STRING,
    ts BIGINT
) WITH (
    KAFKA_TOPIC='nasabah_bank',
    VALUE_FORMAT='JSON',
    PARTITIONS=2
);
