# Meshtastic reader-thread ingress pressure

`tests/test_meshtastic_ingress_pressure.py` reproduces a bounded **synthetic**
callback burst against the real adapter in `connection_type="fake"`. A blocked
canonical publisher holds all submissions so they cannot complete before the
high-water mark is observed. Calls are made from a separate thread, matching the
SDK's synchronous reader callback ownership. Each emitted frame has a distinct
native ID and passes the actual classifier/codec path.

Run the measurement without a radio or network:

```sh
MEDRE_PRESSURE_REPORT_DIR=./pressure-results pytest \
  tests/test_meshtastic_ingress_pressure.py -q
```

The current source creates one `run_coroutine_threadsafe` future per eligible
packet before downstream async admission. With 32 and 256 distinct blocked
callbacks, respectively 32 and 256 futures remained outstanding; every event
published after release, with none retained after drain. A separate test stops
while a publication is held and confirms that its underlying publisher task
exits through cancellation, even after the future tracking set is cleared.
Releasing the sink cannot complete a publication after stop, and a reader-thread
callback after stop cannot schedule another future. Reports record counts, elapsed local time and tier without message
bodies. Elapsed times are **not** throughput or soak guarantees.

This is evidence of linear scheduler pressure, not evidence of dropped packets,
hardware ingress rates, or a safe queue size. The SDK reader thread cannot be
blocked indefinitely; `run_coroutine_threadsafe` cannot provide synchronous
flow control in all contexts. A producer cap without an explicit overflow
contract would turn overload into silently lost mesh messages. Before any
runtime limit, specify and test overload visibility, lifecycle ownership,
callback-thread safety, dedup behavior and sustainable drain rates. The
outbound paced queue's capacity is a separate contract.
