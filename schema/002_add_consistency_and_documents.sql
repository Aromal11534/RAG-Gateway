ALTER TABLE vector_items ADD (
    revision       NUMBER(20) DEFAULT 0 NOT NULL,
    content_hash   VARCHAR2(64 CHAR),
    document_id    VARCHAR2(512 CHAR),
    chunk_index    NUMBER
);

CREATE INDEX vector_items_document_idx
ON vector_items (namespace, document_id, chunk_index);
