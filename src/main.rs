use futures::{self, future::join_all};
use futures_time::future::FutureExt;
use rand::{thread_rng, Rng};
use redis::aio::ConnectionLike;
use std::{
    cmp::max,
    sync::{atomic::AtomicU32, Arc},
};

// Connection constants - these should be adjusted to fit your connection.
const HOST: &str = "localhost";
const PORT: u16 = 6379;

// Benchmark constants - adjusting these will change the meaning of the benchmark.
const PROB_GET: f64 = 0.8;
const PROB_GET_EXISTING_KEY: f64 = 0.8;
const SIZE_GET_KEYSPACE: u32 = 3_750_000;
const SIZE_SET_KEYSPACE: u32 = 3_000_000;

#[tokio::main]
async fn main() {
    let concurrent_tasks: Vec<u32> = vec![1, 10, 100, 1000];
    let data_size_in_bytes: Vec<usize> = vec![100, 4000];
    for data_size in data_size_in_bytes {
        fill_db(data_size).await;
        println!("Done filling DB with values sized {data_size} bytes");
        for concurrent_tasks_count in concurrent_tasks.iter() {
            let counter = Arc::new(AtomicU32::new(0));
            let number_of_operations = max(100000, concurrent_tasks_count * 10000);

            let connection = get_connection().await;

            let mut stopwatch = stopwatch::Stopwatch::start_new();
            join_all((0..*concurrent_tasks_count).into_iter().map(|_| async {
                perform_benchmark(
                    connection.clone(),
                    counter.clone(),
                    number_of_operations,
                    data_size,
                )
                .await;
            }))
            .await;
            stopwatch.stop();
            println!(
                "Results for {} concurrent actions and {} actions:",
                concurrent_tasks_count, number_of_operations
            );
            println!(
                "TPS: {}, elapsed time: {}ms",
                number_of_operations as i64 * 1000 / stopwatch.elapsed_ms(),
                stopwatch.elapsed_ms()
            );
        }
    }
}

fn generate_random_string(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

async fn fill_db(data_size: usize) {
    const CONCURRENT_OPRATIONS: u32 = 10_000;
    const VALUES_FOR_EACH_OPERATION: u32 = SIZE_SET_KEYSPACE / CONCURRENT_OPRATIONS;
    let connection = get_connection().await;
    join_all((0..CONCURRENT_OPRATIONS).into_iter().map(|index| {
        let mut connection = connection.clone();
        async move {
            let mut buffer = itoa::Buffer::new();
            for i in 0..VALUES_FOR_EACH_OPERATION {
                let _: () = redis::cmd("SET")
                    .arg(buffer.format(i + index * VALUES_FOR_EACH_OPERATION))
                    .arg(generate_random_string(data_size))
                    .query_async(&mut connection)
                    .await
                    .unwrap();
            }
        }
    }))
    .await;
}

async fn get_connection() -> impl ConnectionLike + Clone {
    // We'll use this if we want to benchmark a CME server
    let client = redis::cluster::ClusterClientBuilder::new(vec![redis::ConnectionInfo {
        addr: redis::ConnectionAddr::TcpTls {
            host: HOST.to_string(),
            port: PORT,
            insecure: false,
        },
        redis: Default::default(),
    }])
    .tls(redis::cluster::TlsMode::Secure)
    .build()
    .unwrap();
    client
        .get_async_connection()
        .timeout(futures_time::time::Duration::from_millis(500))
        .await
        .expect("connection timed out, please check the HOST const")
        .unwrap()

    // We'll use this if we want to benchmark a CMD server
    // let client = redis::Client::open(redis::ConnectionInfo {
    //     addr: redis::ConnectionAddr::TcpTls {
    //         host: HOST.to_string(),
    //         port: PORT,
    //         insecure: false,
    //     },
    //     redis: Default::default(),
    // })
    // .unwrap();
    // client
    //     .get_multiplexed_async_connection()
    //     .timeout(futures_time::time::Duration::from_millis(500))
    //     .await
    //     .expect("connection timed out, please check the HOST const")
    //     .unwrap()
}

async fn perform_benchmark(
    mut connection: impl ConnectionLike,
    counter: Arc<AtomicU32>,
    number_of_operations: u32,
    data_size: usize,
) {
    let mut buffer = itoa::Buffer::new();
    while counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) < number_of_operations {
        perform_operation(&mut connection, &mut buffer, data_size).await;
    }
}

async fn perform_operation(
    connection: &mut impl ConnectionLike,
    buffer: &mut itoa::Buffer,
    data_size: usize,
) {
    let mut cmd = redis::Cmd::new();
    if rand::thread_rng().gen_bool(PROB_GET) {
        if rand::thread_rng().gen_bool(PROB_GET_EXISTING_KEY) {
            cmd.arg("GET")
                .arg(buffer.format(thread_rng().gen_range(0..SIZE_SET_KEYSPACE)));
        } else {
            cmd.arg("GET")
                .arg(buffer.format(thread_rng().gen_range(SIZE_SET_KEYSPACE..SIZE_GET_KEYSPACE)));
        }
    } else {
        cmd.arg("SET")
            .arg(buffer.format(thread_rng().gen_range(0..SIZE_SET_KEYSPACE)))
            .arg(generate_random_string(data_size));
    };
    connection.req_packed_command(&cmd).await.unwrap();
}
