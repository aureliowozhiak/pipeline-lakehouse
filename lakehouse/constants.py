"""Nomes de colunas técnicas e de tabelas compartilhados entre as camadas."""

CORRUPT_COL = "_corrupt_record"

# Metadados de origem gravados na Bronze (e propagados em parte para a Silver)
META_RECORD_ID = "_record_id"
META_SOURCE_FILE = "_source_file"
META_SOURCE_PATH = "_source_path"
META_SOURCE_SIZE = "_source_size_bytes"
META_SOURCE_HASH = "_source_hash"
META_SOURCE_FORMAT = "_source_format"
META_INGESTION_TS = "_ingestion_timestamp"
META_BATCH_ID = "_batch_id"

BRONZE_METADATA = [
    META_RECORD_ID,
    META_SOURCE_FILE,
    META_SOURCE_PATH,
    META_SOURCE_SIZE,
    META_SOURCE_HASH,
    META_SOURCE_FORMAT,
    META_INGESTION_TS,
    META_BATCH_ID,
]

# Colunas técnicas adicionadas na Silver
SILVER_IS_DUPLICATE = "_is_duplicate"
SILVER_DUPLICATE_COUNT = "_duplicate_count"
SILVER_ROW_HASH = "_row_hash"
SILVER_ISSUES = "_quality_issues"
SILVER_HAS_ISSUES = "_has_quality_issues"
SILVER_PROCESSED_AT = "_silver_processed_at"

RESERVED_COLUMNS = set(BRONZE_METADATA) | {
    CORRUPT_COL,
    SILVER_IS_DUPLICATE,
    SILVER_DUPLICATE_COUNT,
    SILVER_ROW_HASH,
    SILVER_ISSUES,
    SILVER_HAS_ISSUES,
    SILVER_PROCESSED_AT,
}

PLAIN_TEXT_COLUMN = "linha"

# Tabelas Gold (uma tabela cada, com a coluna "dataset")
GOLD_TABLES = [
    "datasets_summary",
    "record_counts",
    "column_profile",
    "value_frequencies",
    "numeric_histograms",
    "quality_metrics",
]

# Tabelas de controle
CONTROL_FILE_REGISTRY = "file_registry"
CONTROL_PIPELINE_RUNS = "pipeline_runs"

STATUS_SUCCESS = "SUCESSO"
STATUS_FAILED = "FALHA"
