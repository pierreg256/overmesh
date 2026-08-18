pub(crate) const CATALOG_PREFIX: &str = "catalog/v1/";
pub(crate) const MAX_BACKEND_OBJECT_NAME_LENGTH: usize = 1_024;

pub(crate) fn catalog_key_length(container: &str, blob: &str) -> usize {
    CATALOG_PREFIX
        .len()
        .saturating_add(2_usize.saturating_mul(container.len()))
        .saturating_add(1)
        .saturating_add(2_usize.saturating_mul(blob.len()))
        .saturating_add(".json".len())
}
