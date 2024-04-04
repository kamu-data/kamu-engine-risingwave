use std::path::Path;

use datafusion::prelude::*;

pub async fn assert_df_eq(df: DataFrame, expected_schema: &str, expected_data: &str) {
    let expected_schema = expected_schema.trim();
    let expected_data = expected_data.trim();

    let arrow_schema = df.schema().clone();
    let batches = df.collect().await.unwrap();

    let data = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    let actual = data.trim();

    assert_eq!(
        expected_data, actual,
        "== Expected ==\n{}\n== Actual ==\n{}\n",
        expected_data, actual,
    );

    let parquet_schema =
        datafusion::parquet::arrow::arrow_to_parquet_schema(&arrow_schema.into()).unwrap();
    let mut parquet_schema_buf = Vec::new();
    datafusion::parquet::schema::printer::print_schema(
        &mut parquet_schema_buf,
        &parquet_schema.root_schema_ptr(),
    );
    let actual = std::str::from_utf8(&parquet_schema_buf).unwrap().trim();

    assert_eq!(
        expected_schema, actual,
        "== Expected ==\n{}\n== Actual ==\n{}\n",
        expected_schema, actual
    );
}

pub async fn assert_parquet_eq(path: &Path, expected_schema: &str, expected_data: &str) {
    let ctx = SessionContext::new();

    let df = ctx
        .read_parquet(
            vec![path.display().to_string()],
            ParquetReadOptions {
                file_extension: "",
                table_partition_cols: Vec::new(),
                parquet_pruning: None,
                skip_metadata: None,
                schema: None,
                file_sort_order: Vec::new(),
            },
        )
        .await
        .unwrap();

    assert_df_eq(df, expected_schema, expected_data).await
}
