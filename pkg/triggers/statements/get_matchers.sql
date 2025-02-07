SELECT
    target_api_version,
    target_kind,
    target_key,
    namespace,
    name,
    label_selectors,
    field_selectors
FROM handler_name_placeholder_matchers
WHERE
    generation = $1 AND
    (namespace = '' OR namespace = $2) AND
    (name = '' OR name = $3);