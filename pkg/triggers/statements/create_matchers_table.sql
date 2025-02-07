CREATE TABLE IF NOT EXISTS handler_name_placeholder_matchers (
    generation INTEGER NOT NULL,
    target_api_version VARCHAR(255) NOT NULL,
    target_kind VARCHAR(255) NOT NULL,
    target_key VARCHAR(515) NOT NULL,
    namespace VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    label_selectors TEXT,
    field_selectors TEXT,
    CONSTRAINT placeholder_unique_entry UNIQUE (generation, target_api_version, target_kind, target_key, namespace, name, label_selectors, field_selectors)
);