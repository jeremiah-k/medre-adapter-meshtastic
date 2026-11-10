# Explicit reaction keys

Drop native tapbacks whose text does not provide an explicit nonempty key.
The codec rejects malformed reaction packets rather than inventing a symbol.
Rendering requires the exact relation key in native and text fallback modes;
body, payload emoji, and placeholder symbols never supply a missing key.
