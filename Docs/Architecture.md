# Algorithmic Trading Platform Architecture

An event-driven, exchange-agnostic algorithmic trading platform built with custom Python OOP adapters, Apache Kafka, TimescaleDB, Redis, FastAPI, and an offline Agentic AI research layer. Designed to run locally via Docker Compose and migrate seamlessly to managed cloud services.

---

## 1. Architecture Diagram

```text
                         [ Exchanges: Binance / Any ]
                            │                    ▲
             (Market Data)  │                    │ (Place / Cancel Orders)
                            ▼                    │
                 ┌────────────────────┐  ┌────────────────────┐
                 │  Ingestion Service │  │  Execution / OMS   │
                 │    (Python OOP)    │  │    (Python OOP)    │
                 └─────────┬──────────┘  └───────▲─────┬──────┘
                           │                     │     │
            market.candles │     orders.approved │     │ orders.fills
                           │                     │     │
===========================▼=====================│=====▼========================
                            APACHE KAFKA (Event Bus)
   Topics: market.candles → indicators → signals → orders.approved → fills
========┬============================┬============================┬=============
        │                            │                            │
        ▼                            ▼                            ▼
┌───────────────────────┐  ┌───────────────────────┐  ┌────────────────────────┐
│   TRADING PIPELINE    │  │   PERSISTENCE LAYER   │  │    FRONTEND SERVING    │
│                       │  │                       │  │                        │
│  [Indicator Engine]   │  │      [DB Writer]      │  │   [WS / SSE Gateway]   │
│          │            │  │  (Consumes candles,   │  │   (Consumes candles,   │
│          ▼ indicators │  │   indicators, fills)  │  │    indicators, fills)  │
│  [Strategy Engine]    │  │           │           │  │           │            │
│          │            │  │           ▼           │  │           │ Live Push  │
│          ▼ signals    │  │ ┌───────────────────┐ │  │           ▼            │
│  [Risk Manager] ◄─────┼──┼─┤  Redis (Live Pos) │ │  │    [Frontend UI]       │
│          │            │  │ └───────────────────┘ │  │           ▲            │
│  (Publishes approved  │  │ ┌───────────────────┐ │  │           │ History &  │
│   orders to Kafka)    │  │ │    TimescaleDB    │─┼──┼─► [REST API (FastAPI)] │
└───────────────────────┘  │ │ • Candles (Aggs)  │ │  │   (Filters, History,   │
                           │ │ • Single Feature  │ │  │    Backtests, AI)      │
                           │ │   Table           │ │  └────────────────────────┘
                           │ │ • Trades & Logs   │ │
                           │ └─────────┬─────────┘ │
                           └───────────┼───────────┘
                                       │ (Historical Data & Saved Results)
                        ┌──────────────┴──────────────┐
                        ▼                             ▼
           ┌─────────────────────────┐   ┌─────────────────────────┐
           │   Backtesting Engine    │◄──┤   Agentic AI Research   │
           │ (Reuses Indicator,      │   │ (SQL, News, Triggers    │
           │  Strategy, Risk classes)│   │  Backtests, Reports)    │
           └─────────────────────────┘   └─────────────────────────┘
```

---

## 2. Layer-by-Layer Explanation

### Zone 1: Exchange Edge (Custom Python OOP)
Only two services communicate with external exchanges. No third-party wrapper like CCXT Pro is used.
* **Ingestion Service:** Implements a `BaseExchange` abstract class (e.g., `BinanceExchange`) to connect via raw WebSockets/REST, normalize payloads into canonical `Candle`/`Tick` objects, and publish to Kafka.
* **Execution / OMS:** Implements a `BaseBroker` abstract class (e.g., `BinanceBroker`, `PaperTradingBroker`). Consumes `orders.approved`, submits/cancels orders on the exchange, and publishes confirmed executions to `orders.fills`.

### Zone 2: Event Bus (Apache Kafka)
Decouples all producers and consumers. Topics are partitioned by `symbol` to guarantee chronological order per asset.
* `market.candles` — Normalized live market data from Ingestion.
* `indicators` — Calculated technical indicator values.
* `signals` — Trade intents emitted by strategies.
* `orders.approved` — Risk-checked orders cleared for execution.
* `orders.fills` — Confirmed trade fills and order status updates.

### Zone 3: Parallel Consumer Lanes

#### Lane A: Trading Pipeline (Hot Path)
Strictly sequential and stateful in memory—never queries the database per tick.
* **Indicator Engine:** Consumes `market.candles`, warms up once from TimescaleDB on startup, computes indicators incrementally in memory, and publishes to `indicators`.
* **Strategy Engine:** Consumes `indicators` (and candles), evaluates rules, and emits trade intents to `signals`.
* **Risk Manager:** Gatekeeper between Strategy and Execution. Consumes `signals`, checks live positions/exposure in **Redis**, enforces sizing/drawdown/kill-switch limits, and publishes valid orders to `orders.approved`.

#### Lane B: Persistence Layer
* **DB Writer:** Single consumer listening to `market.candles`, `indicators`, and `orders.fills` to batch-write into storage.
* **Redis:** Holds low-latency live state (current open positions, account balance, daily PnL, kill-switch flags) for the Risk Manager.
* **TimescaleDB:** Primary time-series database containing:
  * `candles_1m` hypertable + **Continuous Aggregates** (`5m`, `15m`, `1h`, `1d`).
  * `market_features` — Single aggregated table storing pre-computed indicator values so downstream consumers never recompute or query separately.
  * `orders`, `trades`, `backtest_results`, and `research_reports`.

#### Lane C: Frontend Serving (Split Path)
* **WS / SSE Gateway:** Consumes Kafka topics (`market.candles`, `indicators`, `orders.fills`) and pushes live updates directly to the browser for real-time charts.
* **REST API (FastAPI):** Separate read API querying TimescaleDB for historical chart loading, multi-asset screener filters, backtest equity curves, and AI research reports.

### Zone 4: Offline & Research Layer
Completely isolated from the live trading hot path.
* **Backtesting Engine:** Replays historical data from TimescaleDB by importing the exact same Python `Indicator`, `Strategy`, and `Risk` classes used in live trading. Saves metrics and trade logs back to TimescaleDB.
* **Agentic AI Research:** Multi-agent workflow (e.g., LangGraph) with read-only SQL access to TimescaleDB, web/news tools, and the ability to trigger the Backtesting Engine to validate hypotheses and publish research reports.

---

## 3. Core Architecture Rules (For Developers & AI Agents)

1. **Exchange Isolation:** All exchange-specific code must live inside `BaseExchange` / `BaseBroker` subclasses. Never leak raw exchange JSON outside Zone 1.
2. **No Per-Tick DB Polling:** Live indicators, strategies, and the real-time UI must consume from Kafka topics, never poll TimescaleDB.
3. **Mandatory Risk Gate:** Strategies emit `signals` only. Only the Risk Manager may publish to `orders.approved`.
4. **Shared Core Logic:** Indicator, Strategy, and Risk classes must live in a shared `core/` module imported by both the Live Pipeline and the Backtesting Engine.
5. **No LLMs on the Hot Path:** Agentic AI runs asynchronously offline; it never blocks or directly triggers live order execution.
6. **Split UI Paths:** Use the Kafka-backed WS/SSE Gateway for real-time updates and the TimescaleDB-backed FastAPI REST service for history and filters.
7. **Cloud-Ready Config:** All endpoints (`KAFKA_BOOTSTRAP_SERVERS`, `DATABASE_URL`, `REDIS_URL`) must be injected via environment variables.

---

## 4. Local-to-Cloud Migration Mapping

| Component | Local (Docker Compose) | Cloud Equivalent |
| :--- | :--- | :--- |
| **Event Bus** | Apache Kafka (KRaft) / Redpanda | Confluent Cloud / AWS MSK / Redpanda Cloud |
| **Time-Series DB** | `timescale/timescaledb` container | Timescale Cloud / AWS RDS Postgres |
| **Live State Cache** | Redis container | Upstash / AWS ElastiCache / Redis Cloud |
| **Services & API** | Local Docker containers | AWS ECS / Google Cloud Run / Kubernetes |