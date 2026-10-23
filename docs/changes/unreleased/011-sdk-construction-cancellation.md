# Signal cooperative SDK construction cancellation

- Pass the owned construction event to serial, TCP and BLE SDK constructors that
  explicitly support cancellation; retain compatibility and late-client disposal
  for released SDKs.
