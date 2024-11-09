#[derive(Debug, Eq, PartialEq)]
pub (crate) enum ColumnType {
    Uuid,
    Int,
    Long,
    Float,
    Double,
    Timestamp,
    Text,
    Bool
}