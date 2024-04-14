use chrono::{TimeZone, Utc};
use indoc::indoc;
use opendatafabric::*;

use super::{data, df_utils, engine};

/////////////////////////////////////////////////////////////////////////////////////////

super::vary_chunks_size_and_delay!(test_aggregation_inf_window_simple);

async fn test_aggregation_inf_window_simple() {
    let tempdir = tempfile::tempdir().unwrap();

    let input_dataset = data::create_dataset(
        tempdir.path(),
        "transactions",
        &["event_time TIMESTAMP", "delta INT"],
        &[
            indoc!(
                r#"
                2000-01-01,10
                2000-01-02,20
                2000-01-03,-10
                2000-01-04,-10
                "# // WM: 2000-01-04 (last elem delayed)
            ),
            indoc!(
                r#"
                2000-02-01,10
                2000-02-02,10
                2000-02-03,-10
                2000-02-04,-10
            "# // WM: 2000-02-04 (last elem delayed)
            ),
            indoc!(
                r#"
                2000-03-01,10
                2000-03-02,-20
                2000-03-03,10
                2000-03-05,-10
                "# // WM: 2000-03-04 (last elem delayed)
            ),
            // No data
            // WM: 2000-03-05 (last elem still delayed - no data in result)
            //
            // No data
            // WM: 2000-03-06 (last elem is finally flushed)
        ],
    );

    let (mut client, server_handle) = engine::start_engine(&tempdir.path().join("store")).await;

    let client_flow = async move {
        let new_data_path = tempdir.path().join("data-1");
        let new_checkpoint_path = tempdir.path().join("checkpoint-1");

        let input = TransformRequestInput {
            dataset_id: DatasetID::new_seeded_ed25519(b"input"),
            dataset_alias: DatasetAlias::try_from("alice/input").unwrap(),
            query_alias: "alice/input".to_string(),
            vocab: DatasetVocabulary::default(),
            offset_interval: Some(OffsetInterval { start: 0, end: 3 }),
            schema_file: input_dataset.data_slices[0].clone(),
            data_paths: vec![input_dataset.data_slices[0].clone()],
            explicit_watermarks: vec![Watermark {
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap(),
                event_time: Utc.with_ymd_and_hms(2000, 1, 4, 0, 0, 0).unwrap(),
            }],
        };

        let request = TransformRequest {
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
                    query: indoc!(
                        r#"
                        select
                            event_time,
                            delta,
                            sum(delta) over (partition by 1 order by event_time asc) as balance
                        from "alice/input"
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
              OPTIONAL INT32 delta;
              OPTIONAL INT64 balance;
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
                +--------+----+----------------------+----------------------+-------+---------+
                | offset | op | system_time          | event_time           | delta | balance |
                +--------+----+----------------------+----------------------+-------+---------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2000-01-01T00:00:00Z | 10    | 10      |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2000-01-02T00:00:00Z | 20    | 30      |
                | 2      | 0  | 2050-01-01T12:00:00Z | 2000-01-03T00:00:00Z | -10   | 20      |
                +--------+----+----------------------+----------------------+-------+---------+
                "#
            ),
        )
        .await;

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 0, end: 2 }),
                new_watermark: Some(Utc.with_ymd_and_hms(2000, 1, 4, 0, 0, 0).unwrap()),
            }
        );

        tracing::warn!("================= ROUND 2 ==================");

        let new_data_path = tempdir.path().join("data-2");
        let prev_checkpoint_path = Some(new_checkpoint_path);
        let new_checkpoint_path = tempdir.path().join("checkpoint-2");

        let input = TransformRequestInput {
            offset_interval: Some(OffsetInterval { start: 4, end: 7 }),
            data_paths: vec![input_dataset.data_slices[1].clone()],
            explicit_watermarks: vec![Watermark {
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap(),
                event_time: Utc.with_ymd_and_hms(2000, 2, 4, 0, 0, 0).unwrap(),
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
                +--------+----+----------------------+----------------------+-------+---------+
                | offset | op | system_time          | event_time           | delta | balance |
                +--------+----+----------------------+----------------------+-------+---------+
                | 3      | 0  | 2050-01-01T12:00:00Z | 2000-01-04T00:00:00Z | -10   | 10      |
                | 4      | 0  | 2050-01-01T12:00:00Z | 2000-02-01T00:00:00Z | 10    | 20      |
                | 5      | 0  | 2050-01-01T12:00:00Z | 2000-02-02T00:00:00Z | 10    | 30      |
                | 6      | 0  | 2050-01-01T12:00:00Z | 2000-02-03T00:00:00Z | -10   | 20      |
                +--------+----+----------------------+----------------------+-------+---------+
                "#
            ),
        )
        .await;

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 3, end: 6 }),
                new_watermark: Some(Utc.with_ymd_and_hms(2000, 2, 4, 0, 0, 0).unwrap()),
            }
        );

        tracing::warn!("================= ROUND 3 ==================");

        let new_data_path = tempdir.path().join("data-3");
        let prev_checkpoint_path = Some(new_checkpoint_path);
        let new_checkpoint_path = tempdir.path().join("checkpoint-3");

        let input = TransformRequestInput {
            offset_interval: Some(OffsetInterval { start: 8, end: 11 }),
            data_paths: vec![input_dataset.data_slices[2].clone()],
            explicit_watermarks: vec![Watermark {
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap(),
                event_time: Utc.with_ymd_and_hms(2000, 3, 4, 0, 0, 0).unwrap(),
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
                +--------+----+----------------------+----------------------+-------+---------+
                | offset | op | system_time          | event_time           | delta | balance |
                +--------+----+----------------------+----------------------+-------+---------+
                | 7      | 0  | 2050-01-01T12:00:00Z | 2000-02-04T00:00:00Z | -10   | 10      |
                | 8      | 0  | 2050-01-01T12:00:00Z | 2000-03-01T00:00:00Z | 10    | 20      |
                | 9      | 0  | 2050-01-01T12:00:00Z | 2000-03-02T00:00:00Z | -20   | 0       |
                | 10     | 0  | 2050-01-01T12:00:00Z | 2000-03-03T00:00:00Z | 10    | 10      |
                +--------+----+----------------------+----------------------+-------+---------+
                "#
            ),
        )
        .await;

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 7, end: 10 }),
                new_watermark: Some(Utc.with_ymd_and_hms(2000, 3, 4, 0, 0, 0).unwrap()),
            }
        );

        tracing::warn!("================= ROUND 4 ==================");

        let new_data_path = tempdir.path().join("data-4");
        let prev_checkpoint_path = Some(new_checkpoint_path);
        let new_checkpoint_path = tempdir.path().join("checkpoint-4");

        let input = TransformRequestInput {
            offset_interval: None,
            data_paths: vec![],
            explicit_watermarks: vec![Watermark {
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap(),
                event_time: Utc.with_ymd_and_hms(2000, 3, 5, 0, 0, 0).unwrap(),
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

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: None,
                new_watermark: Some(Utc.with_ymd_and_hms(2000, 3, 5, 0, 0, 0).unwrap()),
            }
        );

        assert!(!new_data_path.exists());

        tracing::warn!("================= ROUND 5 ==================");

        let new_data_path = tempdir.path().join("data-5");
        let prev_checkpoint_path = Some(new_checkpoint_path);
        let new_checkpoint_path = tempdir.path().join("checkpoint-5");

        let input = TransformRequestInput {
            offset_interval: None,
            data_paths: vec![],
            explicit_watermarks: vec![Watermark {
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap(),
                event_time: Utc.with_ymd_and_hms(2000, 3, 6, 0, 0, 0).unwrap(),
            }],
            ..input
        };
        let request = TransformRequest {
            query_inputs: vec![input.clone()],
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
                +--------+----+----------------------+----------------------+-------+---------+
                | offset | op | system_time          | event_time           | delta | balance |
                +--------+----+----------------------+----------------------+-------+---------+
                | 11     | 0  | 2050-01-01T12:00:00Z | 2000-03-05T00:00:00Z | -10   | 0       |
                +--------+----+----------------------+----------------------+-------+---------+
                "#
            ),
        )
        .await;

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 11, end: 11 }),
                new_watermark: Some(Utc.with_ymd_and_hms(2000, 3, 6, 0, 0, 0).unwrap()),
            }
        );
    };

    engine::await_client_server_flow!(server_handle, client_flow);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[ignore = "Retractions and corrections in sources are not yet supported (see source comments)"]
#[test_log::test(tokio::test)]
async fn test_aggregation_inf_window_changelog() {
    let tempdir = tempfile::tempdir().unwrap();

    let input_dataset = data::create_dataset_snapshot(
        tempdir.path(),
        "populations",
        &["state STRING", "city STRING", "population INT"],
        &["state", "city"],
        &[
            (
                indoc!(
                    r#"
                    S1,A,10
                    S1,B,20
                    S2,C,30
                    "#
                ),
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(),
            ),
            (
                // -C S1,A,10
                // +C S1,A,11
                indoc!(
                    r#"
                    S1,A,11
                    S1,B,20
                    S2,C,30
                    "#
                ),
                Utc.with_ymd_and_hms(2000, 1, 2, 0, 0, 0).unwrap(),
            ),
            (
                // -R S2,C,30
                indoc!(
                    r#"
                    S1,A,11
                    S1,B,20
                    "#
                ),
                Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap(),
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
            query_alias: "populations".to_string(),
            vocab: DatasetVocabulary::default(),
            offset_interval: Some(OffsetInterval { start: 0, end: 2 }),
            schema_file: input_dataset.data_slices[0].clone(),
            data_paths: vec![input_dataset.data_slices[0].clone()],
            explicit_watermarks: vec![],
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
                            event_time,
                            state,
                            sum(population) over (
                                partition by state
                                order by event_time
                            ) as population
                        from populations
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
            "#
        );

        tracing::warn!("================= ROUND 1 ==================");

        let res = client.execute_transform(request.clone()).await.unwrap();

        assert!(!new_data_path.exists());

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: None,
                new_watermark: None,
            }
        );

        tracing::warn!("================= ROUND 2 ==================");

        let new_data_path = tempdir.path().join("data-2");
        let prev_checkpoint_path = Some(new_checkpoint_path);
        let new_checkpoint_path = tempdir.path().join("checkpoint-2");

        let input = TransformRequestInput {
            offset_interval: Some(OffsetInterval { start: 3, end: 4 }),
            data_paths: vec![input_dataset.data_slices[1].clone()],
            explicit_watermarks: vec![Watermark {
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap(),
                event_time: Utc.with_ymd_and_hms(2000, 1, 1, 12, 0, 0).unwrap(),
            }],
            ..input
        };
        let request = TransformRequest {
            query_inputs: vec![input.clone()],
            next_offset: 0,
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
                "#
            ),
        )
        .await;

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 2, end: 2 }),
                new_watermark: None,
            }
        );

        tracing::warn!("================= ROUND 3 ==================");

        let new_data_path = tempdir.path().join("data-3");
        let prev_checkpoint_path = Some(new_checkpoint_path);
        let new_checkpoint_path = tempdir.path().join("checkpoint-3");

        let input = TransformRequestInput {
            offset_interval: Some(OffsetInterval { start: 5, end: 5 }),
            data_paths: vec![input_dataset.data_slices[2].clone()],
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
                "#
            ),
        )
        .await;

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 3, end: 3 }),
                new_watermark: None,
            }
        );
    };

    engine::await_client_server_flow!(server_handle, client_flow);
}

/////////////////////////////////////////////////////////////////////////////////////////
