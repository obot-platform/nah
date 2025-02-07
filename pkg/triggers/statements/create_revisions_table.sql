CREATE TABLE IF NOT EXISTS handler_name_latest_revisions
(
    kind VARCHAR(255) PRIMARY KEY,
    revision INTEGER NOT NULL,
    generation INTEGER
);
