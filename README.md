# Java Interview Notes

Interview-ready answers for **Mid-Level, Senior, and Lead** Java roles.

Each question uses the same four-part structure:

1. **Direct Core Answer** — 2–3 sentences you can say out loud.
2. **Under-the-Hood Details** — JVM, I/O, threading, latency math, failure modes.
3. **Code Example** — idiomatic Java (21 unless noted).
4. **Interview Pro-Tips & Traps** — gotchas and typical follow-ups.

## Contents

| Topic | File |
|--------|------|
| REST API is 30s because it calls 3 downstream APIs — how do you optimize it? | [docs/rest-api-downstream-latency.md](docs/rest-api-downstream-latency.md) |

## Level legend

**Mid** — name the technique (parallelize, timeout, cache).  
**Senior** — latency math, thread pools, timeouts, partial failure.  
**Lead** — SLOs, bulkheads, product trade-offs, when *not* to call three services on the request path.
