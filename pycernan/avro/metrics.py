from prometheus_client.metrics import Counter, Histogram

PREFIX = "pycernan"


def p(n):
    return PREFIX + "_" + n


# Generates buckets ranging from 64 bytes to 1MB, in powers of 2
SIZE_BUCKETS = [2 ** i for i in range(6, 21)]

ack_count = Counter(p('ack_count'), "Number of acknowledgements received.")
ack_invalid_count = Counter(p('ack_invalid_count'), "Number of invalid acknowledgements received.")
ack_latency = Histogram(p('ack_latency'), "Acknowledgement latency in seconds.")
ack_request_count = Counter(p('ack_request_count'), "Number of acknowledgements requested.")

bytes_sent = Counter(p('bytes_sent'), "Total bytes sent.")
bytes_received = Counter(p('bytes_recv'), "Total bytes received.")

conn_create_count = Counter(p('conn_create_count'), "Number of connections established by connection pool.")
conn_close_count = Counter(p('conn_close_count'), "Number of connections closed by connection pool.")
conn_failure_count = Counter(p('conn_failure_count'), "Number of failures to yield a connection from the pool.")
conn_acquire_latency = Histogram(p('conn_acquire_count'), "Connection acquisition latency")

event_size_bytes = Histogram(p('event_size_bytes'), "Histogram of event sizes in bytes", buckets=SIZE_BUCKETS)

publish_count = Counter(p('publish_count'), "Number of events published.")
publish_failure_count = Counter(p('publish_failure_count'), "Number of events that failed to publish successfully.")
publish_latency = Histogram(p('publish_latency'), "Publish latency in seconds.")
