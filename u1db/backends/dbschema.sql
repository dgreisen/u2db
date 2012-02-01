-- Database schema
CREATE TABLE transaction_log (
    generation INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id TEXT
);
CREATE TABLE document (
    doc_id TEXT PRIMARY KEY,
    doc_rev TEXT,
    doc TEXT
);
CREATE TABLE document_fields (
    doc_id TEXT,
    field_name TEXT,
    value TEXT
);
CREATE INDEX document_fields_field_value_doc_idx
    ON document_fields(field_name, value, doc_id);

CREATE TABLE sync_log (
    replica_uid TEXT PRIMARY KEY,
    known_generation INTEGER
);
CREATE TABLE conflicts (
    doc_id TEXT,
    doc_rev TEXT,
    doc TEXT,
    CONSTRAINT conflicts_pkey PRIMARY KEY (doc_id, doc_rev)
);
CREATE TABLE index_definitions (
    name TEXT,
    offset INT,
    field TEXT,
    CONSTRAINT index_definitions_pkey PRIMARY KEY (name, offset)
);
CREATE TABLE u1db_config (
    name TEXT PRIMARY KEY,
    value TEXT
);
INSERT INTO u1db_config VALUES ('sql_schema', '0');
