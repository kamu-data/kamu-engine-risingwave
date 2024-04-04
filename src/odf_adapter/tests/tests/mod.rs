mod data;
mod df_utils;
mod engine;
mod test_aggregation_inf_window;
mod test_aggregation_tumble;
mod test_error_handling;
mod test_flush;
mod test_join;
mod test_top_n;

macro_rules! vary_chunks_size_and_delay {
    ($test_fn:ident) => {
        mod $test_fn {
            use super::$test_fn;

            #[test_log::test(tokio::test)]
            async fn big_chunks_no_delay() {
                std::env::set_var("RW_ODF_SOURCE_MAX_RECORDS_PER_CHUNK", "10");
                $test_fn().await;
                std::env::set_var("RW_ODF_SOURCE_MAX_RECORDS_PER_CHUNK", "");
            }

            #[test_log::test(tokio::test)]
            async fn small_chunks_no_delay() {
                std::env::set_var("RW_ODF_SOURCE_MAX_RECORDS_PER_CHUNK", "1");
                $test_fn().await;
                std::env::set_var("RW_ODF_SOURCE_MAX_RECORDS_PER_CHUNK", "");
            }

            #[test_log::test(tokio::test)]
            async fn small_chunks_with_delay() {
                std::env::set_var("RW_ODF_SOURCE_MAX_RECORDS_PER_CHUNK", "1");
                std::env::set_var("RW_ODF_SOURCE_SLEEP_CHUNK_MS", "200");
                $test_fn().await;
                std::env::set_var("RW_ODF_SOURCE_MAX_RECORDS_PER_CHUNK", "");
                std::env::set_var("RW_ODF_SOURCE_SLEEP_CHUNK_MS", "");
            }
        }
    };
}

pub(crate) use vary_chunks_size_and_delay;
