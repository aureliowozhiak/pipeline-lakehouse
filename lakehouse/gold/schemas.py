"""Schemas das tabelas Gold (todas têm a coluna "dataset")."""

from pyspark.sql import types as T

S, L, I, D, B, TS = T.StringType(), T.LongType(), T.IntegerType(), T.DoubleType(), T.BooleanType(), T.TimestampType()


def _schema(*fields):
    return T.StructType([T.StructField(name, dtype, True) for name, dtype in fields])


GOLD_SCHEMAS = {
    "datasets_summary": _schema(
        ("dataset", S), ("source_files", I), ("source_file_names", S), ("source_formats", S),
        ("row_count", L), ("column_count", I), ("numeric_columns", I), ("categorical_columns", I),
        ("text_columns", I), ("datetime_columns", I), ("boolean_columns", I),
        ("duplicate_rows", L), ("duplicate_pct", D), ("rows_with_issues", L), ("rows_with_issues_pct", D),
        ("null_cells", L), ("total_cells", L), ("completeness_pct", D),
        ("last_ingestion", TS), ("updated_at", TS),
    ),
    "record_counts": _schema(
        ("dataset", S), ("source_file", S), ("source_format", S), ("row_count", L),
        ("duplicate_rows", L), ("rows_with_issues", L), ("batch_id", S), ("ingested_at", TS),
    ),
    "column_profile": _schema(
        ("dataset", S), ("position", I), ("column", S), ("original_column", S), ("data_type", S),
        ("kind", S), ("inference_rule", S), ("inference_rate", D),
        ("row_count", L), ("non_null_count", L), ("null_count", L), ("null_pct", D),
        ("distinct_count", L), ("distinct_is_approx", B), ("distinct_pct", D),
        ("min_value", S), ("max_value", S), ("mean", D), ("stddev", D), ("p25", D), ("median", D), ("p75", D),
        ("min_length", I), ("max_length", I), ("avg_length", D),
        ("trimmed_count", L), ("nullified_count", L), ("failed_conversions", L),
    ),
    "value_frequencies": _schema(
        ("dataset", S), ("column", S), ("rank", I), ("value", S), ("count", L), ("pct", D),
    ),
    "numeric_histograms": _schema(
        ("dataset", S), ("column", S), ("bin_index", I), ("bin_start", D), ("bin_end", D),
        ("count", L), ("pct", D),
    ),
    "quality_metrics": _schema(
        ("dataset", S), ("column", S), ("metric", S), ("description", S), ("count", L), ("pct", D),
    ),
}
