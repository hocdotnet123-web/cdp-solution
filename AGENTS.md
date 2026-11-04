# AGENTS.md — Real-Time CDP Engineering & Behavioral Guidelines

Always-on engineering, architectural, and behavioral standards for the **Real-Time CDP & On-Site Execution Engine** (`> 2,000 clicks/s` sustained, `10,000 clicks/s` burst, `100,000 CCU`).
Tradeoff: Bias toward architectural isolation, predictable p95 latency, correctness, and simplicity over speculative abstractions.

## 1. Think Before Coding
- State assumptions explicitly. If requirements, schemas, or edge cases are ambiguous, stop and ask.
- When multiple architectural or data-modeling tradeoffs exist, present brief pros/cons instead of picking silently.
- Push back if a requested change violates hot-path latency SLAs or bounded-context isolation, and propose a safer alternative.

## 2. Strict Architecture & Data Isolation Invariants (Non-Negotiable)
- **Polyrepo Codebase Isolation:** Backend, Control Plane Frontend, and Edge SDKs live in strictly separated Git repositories (`cdp-backend`, `cdp-admin-portal`, `cdp-web-sdk`) — never mix Java/Gradle and Node/TypeScript toolchains in the same repository. Synchronize cross-repo payloads via versioned JSON Schema / HTTP & Kafka contracts.
- **1-to-1 Database & Redis Ownership:** Never connect two microservices to the same `PostgreSQL` database, `Redis` cluster, or `S3` bucket:
  - `admin-portal-service` owns `admin-postgres` & `S3 (static-bundles)` exclusively.
  - `ingestion-gateway` is 100% stateless (no DB, no Redis).
  - `realtime-stream-processor` owns `scv-redis` & `identity-postgres` exclusively.
  - `campaign-delivery-service` owns `campaign-redis` (& optional `campaign-postgres`) exclusively.
  - `lakehouse-sync-worker` owns `S3 (cdp-datalake)` (& optional `ClickHouse`) exclusively.
- **Zero Synchronous Inter-Service RPC:** Services must never call each other via synchronous HTTP/gRPC. Propagate config via **Kafka Compacted Topics** (`config.schemas`, `config.stream.rules`, `config.campaigns`) and state via **Event-Carried State Transfer** (`campaign.state.events`, `scv.warm.updates`).

## 3. Hot-Path Performance & Scale Guardrails (`> 2k clicks/s`, `100k CCU`)
- **Zero Relational Queries on the Hot Path:** Never query `PostgreSQL` during `/v1/collect`, `/v1/decide`, `/v1/reactions`, or per-event stream processing (except cold cache-miss hydration in `realtime-stream-processor`). Serve schemas, rules, campaign definitions, and UI templates from **L1 in-memory caches** updated via Kafka.
- **Batch I/O Only:** Use async batched Kafka producers, Redis pipelining/Lua scripts, and micro-batched `S3` Parquet sinks (`1–5 min` flush). Never write per-event objects to `S3` or allocate unbounded buffers in memory (`O(1)` / bounded-window streaming over `O(N)` full loads).
- **Redis Cluster & Multi-Tab Concurrency:**
  - Always wrap user-scoped Redis keys with hash tags (e.g., `camp:ustate:{master_profile_id}`) so multi-key **Atomic Lua Scripts** never fail with `CROSSSLOT`.
  - State transitions, multi-tier **Frequency Capping**, and **Cooldown** checks must execute atomically inside Redis Lua to prevent multi-tab race conditions.
- **Stream Determinism & Ordering:** Partition behavioral Kafka topics consistently by user routing key (`master_profile_id` or `anonymous_id`) and enforce `event_id` deduplication before mutating counters.

## 4. Boundary Defense, Consent & Failure Isolation
- **Edge Validation & DLQ:** Validate against the canonical event schema at `ingestion-gateway`. Route malformed payloads to `.dlq` topics — never let poison messages stall Kafka consumers.
- **Real-Time Consent Gate:** Enforce `consent_flags` before processing. When `personalization = false`, strip/hash PII before `S3` analytics sink, bypass `RealtimeService` / `scv-redis`, and purge active personalized `camp:ustate` entries on mid-session revocation.
- **Internal Simplicity:** Once an event or state passes boundary validation, trust internal typed invariants rather than repeating redundant defensive checks deep inside core functions.

## 5. Simplicity & Surgical Changes
- Write the minimum, cleanest code that solves the task completely — no speculative abstractions, premature interfaces, or unrequested configurability.
- Touch only lines directly required by the task; match existing naming and project layout; never leave dead code, unused imports, debug prints, or commented-out blocks.

## 6. Verification & Documentation Standards
- Define verifiable criteria before multi-step coding (happy path, malformed/DLQ path, multi-tab concurrency, and opt-out/stitching edge cases) and verify tests pass before finishing.
- **Language & Terminology:** When writing Vietnamese documentation or explanations, keep standard English technical, distributed-systems, and CDP/MarTech terms in original English (e.g., *Identity Resolution*, *Session Stitching*, *Sliding Window*, *CEP*, *Campaign Arbitration*, *Atomic Lock*, *Frequency Capping*, *Closed-Loop Reaction*) rather than translating them into awkward Vietnamese.
