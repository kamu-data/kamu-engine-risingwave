use chrono::{TimeZone, Utc};
use indoc::indoc;
use opendatafabric::*;

use super::{data, df_utils, engine};

/////////////////////////////////////////////////////////////////////////////////////////

super::vary_chunks_size_and_delay!(test_aggregation_tumble);

async fn test_aggregation_tumble() {
    let tempdir = tempfile::tempdir().unwrap();

    let input_dataset = data::create_dataset(
        tempdir.path(),
        "daily-cases",
        &["event_time TIMESTAMP", "province STRING"],
        &[
            // After this chunk we expect only 01 and 02 to be processed while
            // 03 is buffered due to the watermark
            indoc!(
                r#"
                2000-01-01,BC
                2000-01-02,BC
                2000-01-02,BC
                2000-01-03,BC
                "# // WM: 2000-01-03
            ),
            // 2000-01-03 cases span between two slices
            indoc!(
                r#"
                2000-01-03,BC
                2000-01-03,BC
                2000-01-04,BC
                "# // WM: 2000-01-05
            ),
        ],
    );

    let (mut client, server_handle) = engine::start_engine(&tempdir.path().join("store")).await;

    let client_flow = async move {
        let new_data_path = tempdir.path().join("data-1");
        let new_checkpoint_path = tempdir.path().join("checkpoint-1");

        let input = TransformRequestInput {
            dataset_id: DatasetID::new_seeded_ed25519(b"input"),
            dataset_alias: DatasetAlias::try_from("input").unwrap(),
            query_alias: "input".to_string(),
            vocab: DatasetVocabulary::default(),
            offset_interval: Some(OffsetInterval { start: 0, end: 3 }),
            schema_file: input_dataset.data_slices[0].clone(),
            data_paths: vec![input_dataset.data_slices[0].clone()],
            explicit_watermarks: vec![Watermark {
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap(),
                event_time: Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap(),
            }],
        };

        let request = TransformRequest {
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
                    query: indoc!(
                        r#"
                        select
                            window_end - interval '1' second as event_time,
                            province,
                            count(1) as daily_cases
                        from tumble(
                            input,
                            event_time,
                            interval '1' day
                        )
                        group by 1, 2
                        emit on window close
                        "#
                    )
                    .to_string(),
                }]),
            }),
            query_inputs: vec![input.clone()],
            next_offset: 0,
            prev_checkpoint_path: None,
            new_checkpoint_path: new_checkpoint_path.clone(),
            new_data_path: new_data_path.clone(),
        };

        let expected_schema = indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
              OPTIONAL BYTE_ARRAY province (STRING);
              OPTIONAL INT64 daily_cases;
            }
            "#
        );

        tracing::warn!("================= ROUND 1 ==================");

        let res = client.execute_transform(request.clone()).await.unwrap();

        df_utils::assert_parquet_eq(
            &new_data_path,
            expected_schema,
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+----------+-------------+
                | offset | op | system_time          | event_time           | province | daily_cases |
                +--------+----+----------------------+----------------------+----------+-------------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2000-01-01T23:59:59Z | BC       | 1           |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2000-01-02T23:59:59Z | BC       | 2           |
                +--------+----+----------------------+----------------------+----------+-------------+
                "#
            ),
        )
        .await;

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 0, end: 1 }),
                new_watermark: Some(Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap()),
            }
        );

        tracing::warn!("================= ROUND 2 ==================");

        let new_data_path = tempdir.path().join("data-2");
        let prev_checkpoint_path = Some(new_checkpoint_path);
        let new_checkpoint_path = tempdir.path().join("checkpoint-2");

        let input = TransformRequestInput {
            offset_interval: Some(OffsetInterval { start: 4, end: 6 }),
            data_paths: vec![input_dataset.data_slices[1].clone()],
            explicit_watermarks: vec![Watermark {
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap(),
                event_time: Utc.with_ymd_and_hms(2000, 1, 5, 0, 0, 0).unwrap(),
            }],
            ..input
        };
        let request = TransformRequest {
            query_inputs: vec![input.clone()],
            next_offset: res.new_offset_interval.unwrap().end + 1,
            prev_checkpoint_path,
            new_checkpoint_path: new_checkpoint_path.clone(),
            new_data_path: new_data_path.clone(),
            ..request
        };
        let res = client.execute_transform(request.clone()).await.unwrap();

        df_utils::assert_parquet_eq(
            &new_data_path,
            expected_schema,
            indoc!(
                r#"
                +--------+----+----------------------+----------------------+----------+-------------+
                | offset | op | system_time          | event_time           | province | daily_cases |
                +--------+----+----------------------+----------------------+----------+-------------+
                | 2      | 0  | 2050-01-01T12:00:00Z | 2000-01-03T23:59:59Z | BC       | 3           |
                | 3      | 0  | 2050-01-01T12:00:00Z | 2000-01-04T23:59:59Z | BC       | 1           |
                +--------+----+----------------------+----------------------+----------+-------------+
                "#
            ),
        )
        .await;

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 2, end: 3 }),
                new_watermark: Some(Utc.with_ymd_and_hms(2000, 1, 5, 0, 0, 0).unwrap()),
            }
        );
    };

    engine::await_client_server_flow!(server_handle, client_flow);
}

/////////////////////////////////////////////////////////////////////////////////////////
