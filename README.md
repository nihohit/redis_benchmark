A simple Redis benchmark, using redis-rs, roughly following the benchmarks [detailed here](https://aws.amazon.com/blogs/database/optimize-redis-client-performance-for-amazon-elasticache/).

Set the path to a node in the HOST field, and if you want to benchmark a CMD cluster, change the code in `get_connection`. The benchmark assumes that the connection requires TLS, and will need to be adjusted for unsecured connections.
