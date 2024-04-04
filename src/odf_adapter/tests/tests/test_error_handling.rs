use std::assert_matches::assert_matches;

use chrono::{TimeZone, Utc};
use indoc::indoc;
use opendatafabric::engine::ExecuteTransformError;
use opendatafabric::*;

use super::{data, engine};

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_error_handling() {
    let tempdir = tempfile::tempdir().unwrap();

    let input = data::create_dataset(
        tempdir.path(),
        "counter",
        &["event_time TIMESTAMP", "counter INT"],
        &[indoc!(
            r#"
            2000-01-01,1
            "#
        )],
    );

    let (mut client, server_handle) = engine::start_engine(&tempdir.path().join("store")).await;

    let client_flow = async move {
        let new_data_path = tempdir.path().join("new_data");

        let res = client
            .execute_transform(TransformRequest {
                dataset_id: DatasetID::new_seeded_ed25519(b"output"),
                dataset_alias: DatasetAlias::try_from("output").unwrap(),
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                vocab: DatasetVocabulary::default(),
                transform: Transform::Sql(TransformSql {
                    engine: "risingwave".to_string(),
                    version: None,
                    temporal_tables: None,
                    query: None,
                    queries: Some(vec![SqlQueryStep {
                        alias: None,
                        query: r#"select does_not_exist from input"#.to_string(),
                    }]),
                }),
                query_inputs: vec![TransformRequestInput {
                    dataset_id: DatasetID::new_seeded_ed25519(b"input"),
                    dataset_alias: DatasetAlias::try_from("input").unwrap(),
                    query_alias: "input".to_string(),
                    vocab: DatasetVocabulary::default(),
                    offset_interval: Some(OffsetInterval { start: 0, end: 29 }),
                    schema_file: input.data_slices[0].clone(),
                    data_paths: input.data_slices,
                    explicit_watermarks: vec![],
                }],
                next_offset: 0,
                prev_checkpoint_path: None,
                new_checkpoint_path: tempdir.path().join("new_checkpoint"),
                new_data_path: new_data_path.clone(),
            })
            .await;

        assert_matches!(
            res,
            Err(ExecuteTransformError::InvalidQuery(
                TransformResponseInvalidQuery { message }
            )) if message.contains("Invalid column")
        );
    };

    engine::await_client_server_flow!(server_handle, client_flow);
}

/////////////////////////////////////////////////////////////////////////////////////////
