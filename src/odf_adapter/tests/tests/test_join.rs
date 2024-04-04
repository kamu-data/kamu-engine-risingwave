use chrono::{TimeZone, Utc};
use indoc::indoc;
use opendatafabric::*;

use super::{data, df_utils, engine};

/////////////////////////////////////////////////////////////////////////////////////////

#[ignore = "RW does not yet support envent-time joins"]
#[test_log::test(tokio::test)]
async fn test_join_one_batch() {
    let tempdir = tempfile::tempdir().unwrap();

    let rate = data::create_dataset(
        tempdir.path(),
        "rate",
        &["event_time TIMESTAMP", "rate BIGINT"],
        &[indoc!(
            r#"
            2000-01-01,1
            2000-01-02,2
            2000-01-03,3
            2000-01-04,4
            2000-01-05,5
            "#
        )],
    );

    let transactions = data::create_dataset(
        tempdir.path(),
        "transactions",
        &["event_time TIMESTAMP", "delta BIGINT"],
        &[indoc!(
            r#"
            2000-01-02,1
            2000-01-02,5
            2000-01-04,5
            2000-01-05,2
            "#
        )],
    );

    let (mut client, server_handle) = engine::start_engine(&tempdir.path().join("store")).await;

    let client_flow = async move {
        let new_data_path = tempdir.path().join("data-1");
        let new_checkpoint_path = tempdir.path().join("checkpoint-1");

        let input_rate = TransformRequestInput {
            dataset_id: DatasetID::new_seeded_ed25519(b"rate"),
            dataset_alias: DatasetAlias::try_from("rate").unwrap(),
            query_alias: "rate".to_string(),
            vocab: DatasetVocabulary::default(),
            offset_interval: Some(OffsetInterval { start: 0, end: 4 }),
            schema_file: rate.data_slices[0].clone(),
            data_paths: vec![rate.data_slices[0].clone()],
            explicit_watermarks: vec![],
        };

        let input_transactions = TransformRequestInput {
            dataset_id: DatasetID::new_seeded_ed25519(b"transactions"),
            dataset_alias: DatasetAlias::try_from("transactions").unwrap(),
            query_alias: "transactions".to_string(),
            vocab: DatasetVocabulary::default(),
            offset_interval: Some(OffsetInterval { start: 0, end: 3 }),
            schema_file: transactions.data_slices[0].clone(),
            data_paths: vec![transactions.data_slices[0].clone()],
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
                queries: Some(vec![
                    SqlQueryStep {
                        alias: Some("current_rate".to_string()),
                        query: indoc!(
                            r#"
                            create materialized view current_rate as
                            select
                                rate as current_rate
                            from (
                                select
                                    rate,
                                    row_number() over (partition by 1 order by event_time) as rn
                                from rate
                            )
                            where rn = 1
                            "#
                        )
                        .to_string(),
                    },
                    // SqlQueryStep {
                    //     alias: Some("tt".to_string()),
                    //     query: indoc!(
                    //         r#"
                    //         create materialized view tt as
                    //         select
                    //             t.event_time,
                    //             t.delta,
                    //             t.delta * r.rate as delta_converted
                    //         from transactions t
                    //         left join rate for system_time as of proctime() r
                    //         "#
                    //     )
                    //     .to_string(),
                    // },
                    SqlQueryStep {
                        alias: None,
                        query: indoc!(
                            r#"
                            select
                                t.event_time,
                                t.delta,
                                r.current_rate as rate,
                                t.delta * r.current_rate as delta_converted
                            from transactions t
                            left join current_rate r
                            "#
                        )
                        .to_string(),
                    },
                ]),
            }),
            query_inputs: vec![input_rate.clone(), input_transactions],
            next_offset: 0,
            prev_checkpoint_path: None,
            new_checkpoint_path: new_checkpoint_path.clone(),
            new_data_path: new_data_path.clone(),
        };

        let expected_schema = indoc!(
            r#"
            "#
        );

        // Round 1
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
                new_offset_interval: Some(OffsetInterval { start: 0, end: 1 }),
                new_watermark: None,
            }
        );
    };

    engine::await_client_server_flow!(server_handle, client_flow);
}

/////////////////////////////////////////////////////////////////////////////////////////
