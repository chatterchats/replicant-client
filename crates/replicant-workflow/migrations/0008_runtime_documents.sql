CREATE TABLE runtime_documents (
    namespace TEXT NOT NULL,
    key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (namespace, key),
    CHECK (length(namespace) BETWEEN 1 AND 128),
    CHECK (length(key) BETWEEN 1 AND 256),
    CHECK (revision >= 0)
) STRICT;

CREATE INDEX idx_runtime_documents_namespace
    ON runtime_documents(namespace, updated_at, key);
