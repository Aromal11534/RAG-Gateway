CREATE TABLE vector_items (
    namespace      VARCHAR2(128 CHAR) NOT NULL,
    id             VARCHAR2(512 CHAR) NOT NULL,
    chunk_text     CLOB NOT NULL,
    metadata_json  CLOB DEFAULT '{}' NOT NULL,
    embedding      VECTOR(384, FLOAT32) NOT NULL,
    revision       NUMBER(20) DEFAULT 0 NOT NULL,
    content_hash   VARCHAR2(64 CHAR),
    document_id    VARCHAR2(512 CHAR),
    chunk_index    NUMBER,
    is_deleted     NUMBER(1) DEFAULT 0 NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at     TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT vector_items_pk PRIMARY KEY (namespace, id),
    CONSTRAINT vector_items_metadata_json_ck CHECK (metadata_json IS JSON)
);

CREATE INDEX vector_items_document_idx
ON vector_items (namespace, document_id, chunk_index);

CREATE VECTOR INDEX vector_items_embedding_hnsw_idx
ON vector_items (embedding)
ORGANIZATION INMEMORY NEIGHBOR GRAPH
DISTANCE COSINE
WITH TARGET ACCURACY 90;
