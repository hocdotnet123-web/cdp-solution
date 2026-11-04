# Real-Time CDP & On-Site Execution Engine

Business specification and strict **Database-per-Service** system architecture for a high-throughput Real-Time Customer Data Platform (`> 2,000 clicks/s` sustained, `10,000 clicks/s` burst, `100,000 Concurrent Users`), inspired by **LG CNS RealCDP** standards.

![Real-Time CDP Architecture](./CDP-Arch.png)

## Repository Contents

- **[lgcns_cdp_expanded_business_spec.md](./lgcns_cdp_expanded_business_spec.md)** — End-to-end Real-Time Processing Business Specification (Edge Ingestion & Consent Gate, Real-Time Identity Resolution & Session Stitching, Session-Scoped & Sliding Window Features, CEP Inactivity Timers, Dynamic Segmentation `ENTER`/`EXIT`, On-Site Arbitration & Multi-Tab Atomic Lua Lock, and `< 250ms` Closed-Loop Survey Reaction).
- **[cdp_strict_microservice_db_architecture.md](./cdp_strict_microservice_db_architecture.md)** — Technical Architecture & Infrastructure Sizing (`1-to-1` exclusive `PostgreSQL` & `Redis` ownership per microservice, Kafka Log-Compacted Config Topics, Event-Carried State Transfer, and HA sizing).
- **[realtime_cdp_strict_architecture.excalidraw](./realtime_cdp_strict_architecture.excalidraw)** — Editable Excalidraw architecture source diagram.
- **[CDP-Arch.png](./CDP-Arch.png)** — Exported architecture diagram image.
