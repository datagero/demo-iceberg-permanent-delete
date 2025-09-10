"""
PII (Personally Identifiable Information) utilities for data deletion and management.
"""


def delete_pii(spark, case_id: str, table_name: str = "demo.default.pii_data"):
    """
    Delete PII for a specific case by setting PII columns to NULL.
    
    This is a common strategy for retaining the record for referential integrity 
    while removing the sensitive information.
    
    Args:
        spark: Spark session
        case_id: The case ID to delete PII for
        table_name: Fully qualified table name (default: "demo.default.pii_data")
    """
    spark.sql(f"""
    UPDATE {table_name}
    SET
        first_name = NULL,
        email_address = NULL,
        secure_txt = NULL
    WHERE case_id = '{case_id}'
    """)
