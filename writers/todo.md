## TODO:
- Dictionary Encoding Enforcement: Currently, DictionaryLevel is not used — consider integrating this to enable per-field optimization.
- Concurrent Write Support: While batching is internal, a goroutine-safe external Write() might be worth considering for future high-throughput use cases.
- Memory Pressure Management: Optionally allow Flush() to be triggered based on memory usage or time interval, not just record count.
- JSON Field Support: Currently stringifies json.RawMessage — you might want to allow raw or structured Arrow JSON fields if needed.
- Stats Export to JSON/YAML: Helpful for logging/reporting pipeline health (not critical but nice-to-have).