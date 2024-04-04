use chrono::{TimeZone, Utc};
use indoc::indoc;
use opendatafabric::*;

use super::{data, df_utils, engine};

/////////////////////////////////////////////////////////////////////////////////////////

super::vary_chunks_size_and_delay!(test_top_n);

async fn test_top_n() {
    let tempdir = tempfile::tempdir().unwrap();

    let input_dataset = data::create_dataset(
        tempdir.path(),
        "player-scores",
        &[
            "event_time TIMESTAMP",
            "match_id INT",
            "player_name STRING",
            "score BIGINT",
        ],
        &[
            indoc!(
                r#"
                2000-01-01,1,Alice,100
                2000-01-01,1,Bob,80
                "# // WM: 2000-01-01+1
            ),
            indoc!(
                r#"
                2000-01-02,2,Alice,70
                2000-01-02,2,Charlie,90
                "# // WM: 2000-01-02+1
            ),
            indoc!(
                r#"
                2000-01-03,3,Bob,60
                2000-01-03,3,Charlie,110
                "# // WM: 2000-01-03+1
            ),
        ],
    );

    let (mut client, server_handle) = engine::start_engine(&tempdir.path().join("store")).await;

    let client_flow = async move {
        let new_data_path = tempdir.path().join("data-1");
        let new_checkpoint_path = tempdir.path().join("checkpoint-1");

        let input = TransformRequestInput {
            dataset_id: DatasetID::new_seeded_ed25519(b"player-scores"),
            dataset_alias: DatasetAlias::try_from("player-scores").unwrap(),
            query_alias: "player_scores".to_string(),
            vocab: DatasetVocabulary::default(),
            offset_interval: Some(OffsetInterval { start: 0, end: 1 }),
            schema_file: input_dataset.data_slices[0].clone(),
            data_paths: vec![input_dataset.data_slices[0].clone()],
            explicit_watermarks: vec![Watermark {
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap(),
                event_time: Utc.with_ymd_and_hms(2000, 1, 1, 1, 0, 0).unwrap(),
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
                queries: Some(vec![
                    SqlQueryStep {
                        alias: Some("leaderboard".to_string()),
                        query: indoc!(
                            r#"
                            create materialized view leaderboard as
                            select
                                *
                            from (
                                select
                                    event_time,
                                    row_number() over (partition by 1 order by score desc) as place,
                                    player_name,
                                    score
                                from player_scores
                            )
                            where place <= 2
                            "#
                        )
                        .to_string(),
                    },
                    SqlQueryStep {
                        alias: None,
                        query: indoc!(
                            r#"
                            select * from leaderboard
                            "#
                        )
                        .to_string(),
                    },
                ]),
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
              OPTIONAL INT64 place;
              OPTIONAL BYTE_ARRAY player_name (STRING);
              OPTIONAL INT64 score;
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
                +--------+----+----------------------+----------------------+-------+-------------+-------+
                | offset | op | system_time          | event_time           | place | player_name | score |
                +--------+----+----------------------+----------------------+-------+-------------+-------+
                | 0      | 0  | 2050-01-01T12:00:00Z | 2000-01-01T00:00:00Z | 1     | Alice       | 100   |
                | 1      | 0  | 2050-01-01T12:00:00Z | 2000-01-01T00:00:00Z | 2     | Bob         | 80    |
                +--------+----+----------------------+----------------------+-------+-------------+-------+
                "#
            ),
        )
        .await;

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 0, end: 1 }),
                new_watermark: Some(Utc.with_ymd_and_hms(2000, 1, 1, 1, 0, 0).unwrap()),
            }
        );

        tracing::warn!("================= ROUND 2 ==================");

        let new_data_path = tempdir.path().join("data-2");
        let prev_checkpoint_path = Some(new_checkpoint_path);
        let new_checkpoint_path = tempdir.path().join("checkpoint-2");

        let input = TransformRequestInput {
            offset_interval: Some(OffsetInterval { start: 2, end: 3 }),
            data_paths: vec![input_dataset.data_slices[1].clone()],
            explicit_watermarks: vec![Watermark {
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap(),
                event_time: Utc.with_ymd_and_hms(2000, 1, 2, 1, 0, 0).unwrap(),
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
                +--------+----+----------------------+----------------------+-------+-------------+-------+
                | offset | op | system_time          | event_time           | place | player_name | score |
                +--------+----+----------------------+----------------------+-------+-------------+-------+
                | 2      | 1  | 2050-01-01T12:00:00Z | 2000-01-01T00:00:00Z | 2     | Bob         | 80    |
                | 3      | 0  | 2050-01-01T12:00:00Z | 2000-01-02T00:00:00Z | 2     | Charlie     | 90    |
                +--------+----+----------------------+----------------------+-------+-------------+-------+
                "#
            ),
        )
        .await;

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 2, end: 3 }),
                new_watermark: Some(Utc.with_ymd_and_hms(2000, 1, 2, 1, 0, 0).unwrap()),
            }
        );

        tracing::warn!("================= ROUND 3 ==================");

        let new_data_path = tempdir.path().join("data-3");
        let prev_checkpoint_path = Some(new_checkpoint_path);
        let new_checkpoint_path = tempdir.path().join("checkpoint-3");

        let input = TransformRequestInput {
            offset_interval: Some(OffsetInterval { start: 4, end: 5 }),
            data_paths: vec![input_dataset.data_slices[2].clone()],
            explicit_watermarks: vec![Watermark {
                system_time: Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap(),
                event_time: Utc.with_ymd_and_hms(2000, 1, 3, 1, 0, 0).unwrap(),
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
                +--------+----+----------------------+----------------------+-------+-------------+-------+
                | offset | op | system_time          | event_time           | place | player_name | score |
                +--------+----+----------------------+----------------------+-------+-------------+-------+
                | 4      | 1  | 2050-01-01T12:00:00Z | 2000-01-01T00:00:00Z | 1     | Alice       | 100   |
                | 5      | 1  | 2050-01-01T12:00:00Z | 2000-01-02T00:00:00Z | 2     | Charlie     | 90    |
                | 6      | 0  | 2050-01-01T12:00:00Z | 2000-01-03T00:00:00Z | 1     | Charlie     | 110   |
                | 7      | 0  | 2050-01-01T12:00:00Z | 2000-01-01T00:00:00Z | 2     | Alice       | 100   |
                +--------+----+----------------------+----------------------+-------+-------------+-------+
                "#
            ),
        )
        .await;

        assert_eq!(
            res,
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 4, end: 7 }),
                new_watermark: Some(Utc.with_ymd_and_hms(2000, 1, 3, 1, 0, 0).unwrap()),
            }
        );
    };

    engine::await_client_server_flow!(server_handle, client_flow);
}

/////////////////////////////////////////////////////////////////////////////////////////
