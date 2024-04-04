use chrono::{TimeZone, Utc};
use indoc::indoc;
use opendatafabric::*;

use super::{data, df_utils, engine};

/////////////////////////////////////////////////////////////////////////////////////////

super::vary_chunks_size_and_delay!(test_flush);

async fn test_flush() {
    let tempdir = tempfile::tempdir().unwrap();

    let input = data::create_dataset(
        tempdir.path(),
        "counter",
        &["event_time TIMESTAMP", "counter INT"],
        &[indoc!(
            r#"
            2000-01-01,1
            2000-01-02,2
            2000-01-03,3
            2000-01-04,4
            2000-01-05,5
            2000-01-06,6
            2000-01-07,7
            2000-01-08,8
            2000-01-09,9
            2000-01-10,10
            2000-02-01,11
            2000-02-02,12
            2000-02-03,13
            2000-02-04,14
            2000-02-05,15
            2000-02-06,16
            2000-02-07,17
            2000-02-08,18
            2000-02-09,19
            2000-02-10,20
            2000-03-01,21
            2000-03-02,22
            2000-03-03,23
            2000-03-04,24
            2000-03-05,25
            2000-03-06,26
            2000-03-07,27
            2000-03-08,28
            2000-03-09,29
            2000-03-10,30
            "#
        )],
    );

    let (mut client, server_handle) = engine::start_engine(&tempdir.path().join("store")).await;

    let client_flow = async move {
        let new_data_path = tempdir.path().join("new_data");

        let res = client
            .execute_transform(TransformRequest {
                dataset_id: DatasetID::new_seeded_ed25519(b"output"),
                dataset_alias: DatasetAlias::try_from("alice/output").unwrap(),
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                vocab: DatasetVocabulary::default(),
                transform: Transform::Sql(TransformSql {
                    engine: "risingwave".to_string(),
                    version: None,
                    temporal_tables: None,
                    query: None,
                    queries: Some(vec![SqlQueryStep {
                        alias: None,
                        query: r#"select * from "alice/input""#.to_string(),
                    }]),
                }),
                query_inputs: vec![TransformRequestInput {
                    dataset_id: DatasetID::new_seeded_ed25519(b"input"),
                    dataset_alias: DatasetAlias::try_from("alice/input").unwrap(),
                    query_alias: "alice/input".to_string(),
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
            .await
            .unwrap();

        df_utils::assert_parquet_eq(
            &new_data_path,
            indoc!(
                r#"
                message arrow_schema {
                  OPTIONAL INT64 offset;
                  REQUIRED INT32 op;
                  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
                  OPTIONAL INT32 counter;
                }
                "#
            ),
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+---------+
                | offset | op | system_time          | event_time           | counter |
                +--------+----+----------------------+----------------------+---------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2000-01-01T00:00:00Z | 1       |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2000-01-02T00:00:00Z | 2       |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2000-01-03T00:00:00Z | 3       |
                | 3      | 0  | 2050-01-01T12:00:00Z | 2000-01-04T00:00:00Z | 4       |
                | 4      | 0  | 2050-01-01T12:00:00Z | 2000-01-05T00:00:00Z | 5       |
                | 5      | 0  | 2050-01-01T12:00:00Z | 2000-01-06T00:00:00Z | 6       |
                | 6      | 0  | 2050-01-01T12:00:00Z | 2000-01-07T00:00:00Z | 7       |
                | 7      | 0  | 2050-01-01T12:00:00Z | 2000-01-08T00:00:00Z | 8       |
                | 8      | 0  | 2050-01-01T12:00:00Z | 2000-01-09T00:00:00Z | 9       |
                | 9      | 0  | 2050-01-01T12:00:00Z | 2000-01-10T00:00:00Z | 10      |
                | 10     | 0  | 2050-01-01T12:00:00Z | 2000-02-01T00:00:00Z | 11      |
                | 11     | 0  | 2050-01-01T12:00:00Z | 2000-02-02T00:00:00Z | 12      |
                | 12     | 0  | 2050-01-01T12:00:00Z | 2000-02-03T00:00:00Z | 13      |
                | 13     | 0  | 2050-01-01T12:00:00Z | 2000-02-04T00:00:00Z | 14      |
                | 14     | 0  | 2050-01-01T12:00:00Z | 2000-02-05T00:00:00Z | 15      |
                | 15     | 0  | 2050-01-01T12:00:00Z | 2000-02-06T00:00:00Z | 16      |
                | 16     | 0  | 2050-01-01T12:00:00Z | 2000-02-07T00:00:00Z | 17      |
                | 17     | 0  | 2050-01-01T12:00:00Z | 2000-02-08T00:00:00Z | 18      |
                | 18     | 0  | 2050-01-01T12:00:00Z | 2000-02-09T00:00:00Z | 19      |
                | 19     | 0  | 2050-01-01T12:00:00Z | 2000-02-10T00:00:00Z | 20      |
                | 20     | 0  | 2050-01-01T12:00:00Z | 2000-03-01T00:00:00Z | 21      |
                | 21     | 0  | 2050-01-01T12:00:00Z | 2000-03-02T00:00:00Z | 22      |
                | 22     | 0  | 2050-01-01T12:00:00Z | 2000-03-03T00:00:00Z | 23      |
                | 23     | 0  | 2050-01-01T12:00:00Z | 2000-03-04T00:00:00Z | 24      |
                | 24     | 0  | 2050-01-01T12:00:00Z | 2000-03-05T00:00:00Z | 25      |
                | 25     | 0  | 2050-01-01T12:00:00Z | 2000-03-06T00:00:00Z | 26      |
                | 26     | 0  | 2050-01-01T12:00:00Z | 2000-03-07T00:00:00Z | 27      |
                | 27     | 0  | 2050-01-01T12:00:00Z | 2000-03-08T00:00:00Z | 28      |
                | 28     | 0  | 2050-01-01T12:00:00Z | 2000-03-09T00:00:00Z | 29      |
                | 29     | 0  | 2050-01-01T12:00:00Z | 2000-03-10T00:00:00Z | 30      |
                +--------+----+----------------------+----------------------+---------+
                "#
            ),
        )
        .await;

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 0, end: 29 }),
                new_watermark: None,
            }
        );
    };

    engine::await_client_server_flow!(server_handle, client_flow);
}

/////////////////////////////////////////////////////////////////////////////////////////
