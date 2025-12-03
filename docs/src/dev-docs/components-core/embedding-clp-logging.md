# Writing structured logs directly to a CLP archive

This note shows how to replace a `printf("rc.stage: %d\n", rc.stage);` style log with
CLP’s writers so the value is recorded in a `.clp` archive instead of the console. It uses the
existing archive/file lifecycle plus the IR (intermediate representation) path for the most
efficient ingestion: you build the log’s logtype and variable payload yourself, so no runtime
parsing is required.

## When to use IR vs. schema vs. raw text
- **IR (`write_log_event_ir`)** – fastest: you construct the logtype template and encoded
  variables yourself and hand them to the archive writer.
- **Schema (`write_msg_using_schema`)** – you already parsed with `log_surgeon` and have a
  `LogEventView`; the writer reuses that parsed view.
- **Raw text (`write_msg`)** – simplest but least CPU-efficient: the writer parses the raw
  string on the fly.

All three methods share the same archive/file/dictionary machinery exposed by
`streaming_archive::writer::Archive`.【F:components/core/src/clp/streaming_archive/writer/Archive.hpp†L85-L186】

## Minimal IR-based example for `rc.stage`
```cpp
#include <chrono>
#include <string>
#include <boost/uuid/random_generator.hpp>
#include "clp/streaming_archive/writer/Archive.hpp"
#include "clp/ir/EncodedTextAst.hpp"
#include "clp/ir/LogEvent.hpp"

using clp::streaming_archive::writer::Archive;
using clp::ir::EncodedTextAst;
using clp::ir::LogEvent;
using clp::ir::eight_byte_encoded_variable_t;

static Archive g_archive;
static bool g_initialized = false;

extern "C" void clp_write_stage(int rc_stage) {
    if (!g_initialized) {
        Archive::UserConfig cfg{};
        cfg.id = boost::uuids::random_generator()();
        cfg.creator_id = cfg.id;
        cfg.creation_num = 0;
        cfg.target_segment_uncompressed_size = 64 * 1024 * 1024;
        cfg.compression_level = 3;          // zstd level
        cfg.output_dir = "/tmp/stages.clp"; // parent dir that will hold the archive dir
        cfg.global_metadata_db = nullptr;
        cfg.print_archive_stats_progress = false;
        g_archive.open(cfg);                 // sets up archive dirs and dictionaries
        g_archive.create_and_open_file("rc.log", 0, cfg.id); // one logical file for this source
        g_initialized = true;
    }

    // Build IR payload: logtype is the template text with a placeholder, encoded_vars holds
    // the integer value we want to persist. No parsing occurs here.
    EncodedTextAst<eight_byte_encoded_variable_t> ast(
            "rc.stage: {i}",            // logtype template; {i} stands for integer slot
            {},                          // dict_vars (none for this simple example)
            {static_cast<eight_byte_encoded_variable_t>(rc_stage)} // encoded_vars
    );

    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    LogEvent<eight_byte_encoded_variable_t> ev(
            now_ms,                      // timestamp in ms
            0,                           // UTC offset (0 if unknown)
            std::move(ast)
    );

    g_archive.write_log_event_ir(ev);      // persist the event into the current logical file
    // Call g_archive.write_dir_snapshot() and g_archive.close() during shutdown to finalize.
}
```
Key points:
- The logtype string (`"rc.stage: {i}"`) captures the structure of the log. CLP dictionaries
  store it once and reuse it across events.
- `encoded_vars` carries the value of `rc.stage` as a 64-bit integer, matching
  `eight_byte_encoded_variable_t`.【F:components/core/src/clp/ir/types.hpp†L8-L23】
- `write_log_event_ir` appends this pre-encoded event without needing to parse the text, making
  it more efficient than raw `write_msg` ingestion. The templated API is declared alongside the
  other write methods in the archive interface.【F:components/core/src/clp/streaming_archive/writer/Archive.hpp†L145-L162】

## Using schema or raw text instead (if preferred)
If you already have a `log_surgeon::LogEventView` for the message, call
`write_msg_using_schema(view)` to reuse the parsed fields. For the simplest drop-in replacement
of `printf`, you can still call `write_msg(timestamp, message, message.size())`, but that path
will re-tokenize the text on every call.【F:components/core/src/clp/streaming_archive/writer/Archive.hpp†L145-L154】

## Lifecycle reminders
- Call `Archive::open` once to configure the archive and `create_and_open_file` once per logical
  source you want to write into. Reuse the same logical file for many messages.
- Periodically (or at shutdown) call `write_dir_snapshot()` then `close()` to flush dictionaries
  and finalize the archive directory so it can be read later. Both operations are provided by the
  archive writer interface.【F:components/core/src/clp/streaming_archive/writer/Archive.hpp†L93-L186】
