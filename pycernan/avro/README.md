# Avro Client

## Features

* Supports Python2, Python3.
* Publish Avro from: dicts, files, and raw blobs.
* Synchronous and asynchronous publication.

## Usage

```python
from pycernan.avro import Client

schema = {
  "namespace": "pycernan.avro.example",
  "type": "record",
  "name": "User",
  "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}

records = [
  {"name": "Foo Bar", "favorite_number": 13, "favorite_color": "Aqua"},
  {"name": "Bar Baz", "favorite_number", 7, "favorite_color": "Greenish Gold"},
]

client = Client() #Connects to localhost:2002 by default.
client.publish(schema, records)

```

## Performance

The following results were gathered on a t2.micro EC2 instance running:

* A single Cernan (version 0.8.7) instance configured as follows:

	```
	directory = "/var/lib/cernan"
	flush-interval = 10

	[sources]
	  [sources.avro.primary]
	  forwards = ["sinks.null"]

	[sinks]
	  [sinks.null]
	```

* A single-threaded `pycernan.avro.Client` publishing as fast as it can..

### Scenarios

All benchmark scenarios measure synchronous publication.  Namely, the round-trip time from submitting an Avro payload until acknowledgement is received from the Avro source confirming the payload has been recorded within Cernan's internal queues.

#### Pregenerated

Avro blobs are pregenerated and published at random.  Data used is the same data used in the unit tests of this project.

### Results

|           Scenario              |         Throughput         |     Latency Min/Mean/Max (microseconds)   |    Limiting Factor     |
|:-------------------------------:|:--------------------------:|:-----------------------------------------:|:-----------------------|
|  [Pregenerated](#pregenerated)  |   ~1.4k blobs / second     |          107 / 462 / 7.6k                 |          CPU           |

