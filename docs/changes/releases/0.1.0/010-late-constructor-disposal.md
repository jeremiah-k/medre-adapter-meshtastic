# Retire native clients after event-loop shutdown cancellation

Native construction retains a thread-safe abandonment marker. Late clients close
inside the constructing thread even when interpreter shutdown cancels the asyncio
executor wrapper. Published results close exactly once through their retained
disposer. Shutdown reports outstanding native setup rather than implying its SDK
thread has terminated; process exit can still await the native constructor timeout.
