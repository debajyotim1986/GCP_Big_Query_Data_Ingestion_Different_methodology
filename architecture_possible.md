# GCP Batch Delta Ingestion — Architecture Compendium

> **Pattern:** Full Snapshot Scan → Delta Detection → DQ Check → Selective Ingestion  
> **Scope:** All viable GCP-native approaches with architecture diagrams, trade-offs, and decision guidance

---

## Table of Contents

| # | Solution | Best For |
|---|---|---|
| [01](#option-1--pure-bigquery-sql--scheduled-queries) | Pure BigQuery SQL + Scheduled Queries | Fast delivery, SQL-first teams |
| [02](#option-2--cloud-dataflow-apache-beam) | Cloud Dataflow (Apache Beam) | Non-BQ sources, TB+ scale |
| [03](#option-3--cloud-composer--bigquery-operators) | Cloud Composer + BigQuery Operators | Multi-step DAG, enterprise orchestration |
| [04](#option-4--dataproc-pyspark--serverless) | Dataproc (PySpark / Serverless) | GCS source, Spark ecosystem |
| [05](#option-5--cloud-run-jobs) | Cloud Run Jobs | Medium scale, low cost, Python-first |
| [06](#option-6--bq--dataplex-dq-enterprise-governed) | BQ + Dataplex DQ | Enterprise governance, lineage, compliance |
| [★](#recommended-hybrid-architecture) | Recommended Hybrid | Most enterprise BQ-to-BQ scenarios |
| [⊞](#decision-matrix) | Decision Matrix | Quick comparison reference |

---

## Option 1 — Pure BigQuery SQL + Scheduled Queries

> Zero-infrastructure — all logic in SQL, scheduled natively in BigQuery

### Architecture

```mermaid
flowchart TD
    SCHED([Cloud Scheduler\nor BQ Scheduled Query])
    SCHED --> STEP1

    subgraph BQENGINE [BigQuery Engine]
        STEP1[Step 1\nFull Snapshot Scan\nWatermark Filter]
        STEP2[Step 2\nMD5 Hash Generation\nSpecific Cols or All Cols]
        STEP3[Step 3\nDelta Detection\nLEFT JOIN vs Target]
        STEP4[Step 4\nDQ Rules\nCTE-based SQL Checks]
        GATE{DQ Passed?}
        STEP5A[Step 5a\nMERGE to Target Table\nINSERT + UPDATE]
        STEP5B[Step 5b\nWrite to Error Table\nFailed Row Count]
        STEP6[Step 6\nUpdate Process Control\nAdvance Watermark]
        STEP7[Step 7\nInsert to Report Table\nRun Summary]
    end

    subgraph TABLES [BQ Tables]
        SRC[(Source Table)]
        TGT[(Target Table)]
        ERR[(Error Table)]
        CTL[(Process Control)]
        RPT[(Report Table)]
    end

    STEP1 --> STEP2 --> STEP3 --> STEP4 --> GATE
    GATE -- YES --> STEP5A
    GATE -- NO --> STEP5B
    STEP5A --> STEP6
    STEP5B --> STEP6
    STEP6 --> STEP7

    STEP1 -. reads .-> SRC
    STEP5A -. writes .-> TGT
    STEP5B -. writes .-> ERR
    STEP6 -. updates .-> CTL
    STEP7 -. inserts .-> RPT
```

### How It Works

All logic lives inside a single BigQuery query chain using CTEs or chained `CREATE TEMP TABLE` statements. The pipeline runs entirely within BQ — no Python, no clusters. Cloud Scheduler or BQ Scheduled Queries triggers execution. BQ handles snapshot scan, hash generation, delta detection, DQ checks, MERGE, and audit writes in sequence.

### Pros and Cons

| ✅ Pros | ❌ Cons |
|---|---|
| Zero infrastructure — no clusters or environments | DQ logic is SQL-only — no ML or cross-system checks |
| Fastest time to production — days not weeks | Dynamic SQL via `EXECUTE IMMEDIATE` gets messy |
| Native BQ performance — petabyte-scale columnar | No unit testing framework for SQL procedures |
| Pay-per-query — no always-on cost | Limited error branching — IF/CASE chains only |
| Full SQL auditability in BQ Job History | Hard to version-control stored procedure bodies |
| BQ Scheduled Queries has built-in retry | No native row-level quarantine routing |

> **Best For:** Source is BigQuery, DQ rules are SQL-expressible, SQL-first team, fast delivery needed.

---

## Option 2 — Cloud Dataflow (Apache Beam)

> Managed distributed processing — best for non-BQ sources and complex DQ

### Architecture

```mermaid
flowchart TD
    TRIGGER([Cloud Scheduler\nor Composer Trigger])
    TRIGGER -- submits job --> READER

    subgraph DATAFLOW [Dataflow Flex Template Job]
        READER[Source Reader\nBoundedSource\nBQ / GCS / Cloud SQL]
        SNAP[Snapshot Transform\nApply watermark filter\nPartition input]
        SIDE[Side Input Load\nTarget PKs and Hashes\nLoaded into memory]
        DELTA[Delta Detection\nCoGroupByKey\nHash comparison]
        DQ[DQ Transform\nCustom DoFn\nPer-row validation]
        ROUTER{Route\nOutput}
        PASS[Target Writer\nBQ Storage Write API\nMERGE mode]
        QUAR[Quarantine Sink\nBQ Dead Letter Table]
        WARN[Warning Logger\nBQ Audit Table]
        AUDIT[Audit Sink\nRun Summary Record]
    end

    subgraph SINKS [BigQuery Sinks]
        TGT[(Target Table)]
        DL[(Dead Letter Table)]
        AUD[(Audit Table)]
        CTL[(Process Control)]
    end

    READER --> SNAP --> SIDE --> DELTA --> DQ --> ROUTER
    ROUTER -- PASS --> PASS
    ROUTER -- QUARANTINE --> QUAR
    ROUTER -- WARN --> WARN
    PASS --> AUDIT
    QUAR --> AUDIT
    WARN --> AUDIT

    PASS -. writes .-> TGT
    QUAR -. writes .-> DL
    AUDIT -. writes .-> AUD
    AUDIT -. updates watermark .-> CTL
```

### How It Works

A Flex Template (containerised Apache Beam job) is triggered per schedule. It reads the full snapshot using a `BoundedSource`, loads target hashes as a **side input**, then routes records through a DQ `DoFn` using `TaggedOutputs` before writing to appropriate sinks. All transforms are distributed across Dataflow workers with auto-scaling.

### Pros and Cons

| ✅ Pros | ❌ Cons |
|---|---|
| Best for non-BQ sources — GCS, Cloud SQL, Pub/Sub | Higher complexity — Beam programming model learning curve |
| True distributed processing — auto-scales to any volume | Startup latency — 2–5 minutes even for small datasets |
| Rich DQ — custom Python/Java DoFn, ML models, APIs | Per-worker-hour cost even for short jobs |
| Native dead-letter routing via TaggedOutputs | Overkill for BQ-to-BQ — native SQL is faster and cheaper |
| Exactly-once delivery guarantees | Side input memory limits for large target hash sets |
| Flex Templates portable across projects | Template images need building and pushing to Artifact Registry |

> **Best For:** Source is GCS/Cloud SQL/external API, dataset is TB+, DQ requires custom Python, multiple output sinks needed.

---

## Option 3 — Cloud Composer + BigQuery Operators

> Best orchestration — multi-step DAG with DQ gate, retries, SLA monitoring

### Architecture

```mermaid
flowchart TD
    SCHED([Cloud Scheduler\nDAG Trigger])
    SCHED --> T1

    subgraph DAG [Cloud Composer DAG - Airflow 2.x]
        T1[Task 1\nload_config\nPythonOperator\nRead table_registry from BQ]
        T2[Task 2\nextract_snapshot\nBigQueryInsertJobOperator\nWatermark-filtered scan to staging]
        T3[Task 3\ndetect_delta\nBigQueryInsertJobOperator\nHash-based MERGE to delta_staging]
        T4[Task 4\nrun_dq_checks\nBigQueryCheckOperator\nor PythonOperator + Great Expectations]
        GATE{BranchPython\nOperator\nDQ Gate}
        T5A[Task 5a\nmerge_to_target\nBigQueryInsertJobOperator\nMERGE delta to target]
        T5B[Task 5b\nwrite_dq_errors\nBigQueryInsertJobOperator\nInsert to error table]
        T6[Task 6\nupdate_watermark\nPythonOperator\nAdvance watermark timestamp]
        T7[Task 7\nwrite_run_report\nBigQueryInsertJobOperator]
        T8[Task 8\nsend_alert\nCloud Monitoring + Slack]
    end

    subgraph STORE [BigQuery Tables]
        STG[(Staging Table)]
        TGT[(Target Table)]
        ERR[(Error Table)]
        CTL[(Process Control)]
        RPT[(Report Table)]
    end

    T1 --> T2 --> T3 --> T4 --> GATE
    GATE -- PASS --> T5A
    GATE -- FAIL --> T5B
    T5A --> T6
    T5B --> T6
    T6 --> T7 --> T8

    T2 -. writes .-> STG
    T5A -. writes .-> TGT
    T5B -. writes .-> ERR
    T6 -. updates .-> CTL
    T7 -. inserts .-> RPT
```

### How It Works

Each pipeline step is a separate Airflow task connected by dependencies. `BranchPythonOperator` acts as a DQ gate — halting or routing based on DQ results. BigQuery does all heavy SQL work; Composer provides orchestration, retry logic, SLA monitoring, and enterprise alerting. XCom passes run summaries between tasks.

### Pros and Cons

| ✅ Pros | ❌ Cons |
|---|---|
| Best orchestration — full DAG, retries, SLAs, branching | Composer environment always-on cost ~$300–800/month |
| Native BQ integration via BigQueryOperator family | Complex dynamic DAGs slow Composer scheduler |
| Great Expectations via GreatExpectationsOperator | Steep learning curve for DAG patterns and XCom |
| Visual DAG monitoring — Airflow UI task-level status | Each task has 30–60s startup overhead |
| XCom for clean inter-task data passing | Not for real-time — batch scheduler only |
| Enterprise alerting — PagerDuty, Slack, email built-in | Worker resource limits for PythonOperator tasks |

> **Best For:** Multi-step pipeline with complex dependencies, enterprise monitoring needed, multiple teams sharing one orchestration platform.

---

## Option 4 — Dataproc (PySpark / Serverless)

> Distributed Spark processing — best for GCS-sourced large-scale data

### Architecture

```mermaid
flowchart TD
    TRIG([Composer Trigger\nor REST API Call])
    TRIG -- DataprocSubmitJobOperator --> SUBMIT

    subgraph SPARK [Dataproc Serverless or Cluster]
        SUBMIT[Job Submission\nPySpark job JAR or Python file]
        READ[PySpark Reader\nspark.read.bigquery\nor spark.read.parquet from GCS]
        FILTER[Watermark Filter\ndf.filter col greater than last_wm]
        HASH[Hash Generation\nwithColumn MD5 concat_ws cols]
        JOIN[Delta Detection\ndf.join on PKs left\nfilter hash mismatch or new record]
        DQ[DQ Checks\nPySpark DataFrame validators\nor PyDeequ rules]
        ROUTER{Split\nDataFrame}
        WRITE[BQ Writer\nspark-bigquery-connector\nor MERGE via BQ job]
        ERRWRITE[Error Writer\nerror_df.write.bigquery\nerror_table]
        WM[Watermark Update\nbq_client.update_watermark]
        AUD[Audit Record\nbq_client.insert_run_summary]
    end

    subgraph STORAGE [Storage]
        GCS[(GCS\nParquet / CSV / Avro)]
        BQ[(BigQuery\nSource and Target)]
    end

    SUBMIT --> READ --> FILTER --> HASH --> JOIN --> DQ --> ROUTER
    ROUTER -- PASS --> WRITE
    ROUTER -- FAIL --> ERRWRITE
    WRITE --> WM
    ERRWRITE --> WM
    WM --> AUD

    READ -. reads .-> GCS
    READ -. reads .-> BQ
    WRITE -. writes .-> BQ
```

### How It Works

PySpark job runs on Dataproc Serverless (recommended) or a standard cluster. Source data from GCS or BQ is read into DataFrames. Delta detection uses a DataFrame join with hash comparison. DQ is applied as column-level filter expressions or PyDeequ rules. Clean delta is written to BQ via the `spark-bigquery-connector`.

### Pros and Cons

| ✅ Pros | ❌ Cons |
|---|---|
| Best for GCS-sourced large data — Parquet/ORC/Avro | Startup time — even Serverless Spark takes 2–4 minutes |
| Full Spark ecosystem — PyDeequ, Spark ML, custom UDFs | Expensive for small jobs — minimum billing unit |
| Serverless Dataproc — no cluster management, pay-per-job | Cluster management for standard clusters — sizing, patching |
| Rich DataFrame API — complex delta logic in clean Python | spark-bigquery-connector writes slower than native BQ |
| Multi-format — Hive, Delta Lake, Iceberg natively | Memory-bound joins with large target hash sets |
| Familiar for data engineers — Spark is industry standard | Overengineered for BQ-to-BQ pipelines |

> **Best For:** Source data is on GCS (Parquet/CSV/Avro), very large dataset, team has Spark expertise, Hive/Iceberg ecosystem integration needed.

---

## Option 5 — Cloud Run Jobs

> Containerised Python batch — low-cost, fast iteration, medium-scale

### Architecture

```mermaid
flowchart TD
    SCHED([Cloud Scheduler\nHTTP trigger])
    SCHED -- invokes --> INIT

    subgraph CRJ [Cloud Run Job - Containerised Python]
        INIT[Job Init\nLoad config from BQ\nGet watermark]
        LOCK[Acquire Lock\nAtomic BQ MERGE\nconcurrency control]
        SNAP[Snapshot Query\nBQ client watermark filter\nincremental window]
        DELTA[Delta Detection\nPolars or Pandas join\nor BQ query-based]
        DQ[DQ Validation\nPandera or Great Expectations\nor custom rule engine]
        GATE{DQ Result}
        ERRLOG[Error Logging\nWrite counts to BQ\nerror_table]
        WRITE[BQ Write\nStorage Write API\nMERGE mode]
        PC[Update Process Control\nAdvance or hold watermark\nbased on outcome]
        RPT[Audit Record\nInsert to report_table]
        CLEAN[Cleanup\nRelease lock\nLog metrics to Cloud Monitoring]
    end

    subgraph INFRA [Infrastructure]
        AR[Artifact Registry\nDocker Image]
        SM[Secret Manager\nService Account Credentials]
    end

    subgraph BQ [BigQuery]
        SRC[(Source Table)]
        TGT[(Target Table)]
        ERR[(Error Table)]
        CTL[(Process Control)]
        RPTBL[(Report Table)]
    end

    INIT --> LOCK --> SNAP --> DELTA --> DQ --> GATE
    GATE -- REJECT failed --> ERRLOG
    GATE -- PASS --> WRITE
    ERRLOG --> PC
    WRITE --> PC
    PC --> RPT --> CLEAN

    SNAP -. queries .-> SRC
    WRITE -. writes .-> TGT
    ERRLOG -. inserts .-> ERR
    PC -. updates .-> CTL
    RPT -. inserts .-> RPTBL
    CRJ -. pulls image .-> AR
    CRJ -. reads secrets .-> SM
```

### How It Works

A containerised Python application runs as a Cloud Run Job triggered by Cloud Scheduler. Uses the BQ Python client for queries and writes. DQ implemented via Pandera, Great Expectations, or custom rules in-memory. BQ Storage Write API handles efficient bulk writes. All state persisted to BQ between runs — fully stateless container.

### Pros and Cons

| ✅ Pros | ❌ Cons |
|---|---|
| Low cost — pay only for execution time | Memory-bound — 32GB max, not for TB+ datasets |
| Simple deployment — one Docker image, one Scheduler job | No distributed processing — single container |
| Full Python flexibility — any library for DQ or transforms | No built-in retry DAG — all-or-nothing retries |
| Fast cold start — seconds vs minutes for Dataflow | No visual pipeline view — no Airflow-style UI |
| Easy local development — run container locally for E2E test | Must push custom metrics to Cloud Monitoring manually |
| Secret Manager integration — credentials cleanly injected | Stateless — all state must persist to BQ/GCS between runs |

> **Best For:** Medium-scale BQ-to-BQ, Python-first team, low-cost requirement, fast iteration cycle.

---

## Option 6 — BQ + Dataplex DQ (Enterprise Governed)

> Full governance — lineage, DQ catalog, regulatory compliance built-in

### Architecture

```mermaid
flowchart TD
    SCHED([Cloud Composer\nDAG Trigger])
    SCHED --> T_SNAP

    subgraph ORCHESTRATION [Cloud Composer DAG]
        T_SNAP[Task 1\nextract_snapshot\nBigQueryInsertJobOperator\nWatermark scan to staging table]
        T_DELTA[Task 2\ndetect_delta\nBigQueryInsertJobOperator\nHash-based delta to delta_staging]
        T_TRIGGER[Task 3\nTrigger Dataplex DQ Scan\nDataplexDataQualityJobTrigger]
        T_POLL[Task 4\nPoll DQ Job Status\nPythonOperator - check BQ results]
        GATE{DQ Gate\nBranchOperator}
        T_MERGE[Task 5a\nmerge_to_target\nBigQueryInsertJobOperator]
        T_HALT[Task 5b\nhalt_and_alert\nRaise exception + Cloud Monitoring]
        T_WM[Task 6\nadvance_watermark\nPythonOperator]
        T_RPT[Task 7\nwrite_report\nBigQueryInsertJobOperator]
    end

    subgraph DATAPLEX [Cloud Dataplex DQ Engine - Managed Serverless]
        DQ_YAML[DQ Rules defined in YAML\nNot Null - Range - Regex\nUniqueness - Referential - Custom SQL]
        DQ_SCAN[Managed DQ Scan\nServerless fully managed execution]
        DQ_RESULT[DQ Results\nRow-level pass and fail scores\nWritten to BQ results table]
        DQ_YAML --> DQ_SCAN --> DQ_RESULT
    end

    subgraph CATALOG [Dataplex Catalog]
        LINEAGE[Column-level Data Lineage\nSource to Target tracking]
        DQ_HIST[DQ Score History\nTrend over time per table]
        CAT[Data Catalog\nTable metadata and DQ health tags]
    end

    T_SNAP --> T_DELTA --> T_TRIGGER --> DATAPLEX
    DATAPLEX --> T_POLL --> GATE
    GATE -- PASS --> T_MERGE
    GATE -- FAIL --> T_HALT
    T_MERGE --> T_WM --> T_RPT

    DATAPLEX -. publishes lineage .-> LINEAGE
    DATAPLEX -. updates scores .-> DQ_HIST
    DATAPLEX -. tags tables .-> CAT
```

### How It Works

Composer orchestrates the staging and MERGE steps. **Dataplex Data Quality** acts as the managed DQ engine — YAML-configured rules run serverlessly against the delta staging table. Results are stored in BQ and surfaced in Dataplex Catalog with lineage tracking and historical DQ scores. No DQ code to write or maintain.

### Pros and Cons

| ✅ Pros | ❌ Cons |
|---|---|
| Enterprise governance built-in — lineage, catalog, DQ scores | Async DQ — Composer must poll; adds 5–15 min latency |
| Zero DQ code — rules defined in YAML | Per-scan pricing can be significant for frequent small runs |
| Historical DQ trending — pass/fail scores over time | Complex cross-system checks require workarounds |
| Column-level lineage from source to target | Dataplex zone/lake/catalog setup adds complexity |
| Regulatory compliance ready — GDPR/CCPA audit trail | Dataplex samples by default — full scan needs explicit config |
| Managed scaling — DQ scans are fully serverless | GCP lock-in — Dataplex DQ rules are not portable |

> **Best For:** Enterprise platform with compliance requirements, data catalog, lineage, regulatory audit trail, and centralised DQ governance across teams.

---

## Recommended Hybrid Architecture

For most enterprise BQ-to-BQ delta ingestion requirements, the optimal pattern combines **Cloud Composer** for orchestration + **Python-generated BigQuery SQL** for processing + a **config-driven DQ rule engine** backed by BQ tables.

```mermaid
flowchart TD
    SCHED([Cloud Scheduler\n2 AM UTC Daily])
    SCHED --> DISCOVER

    subgraph COMPOSER [Cloud Composer - Orchestration Layer]
        DISCOVER[discover_tables\nPythonOperator\nLoad all active table_registry rows from BQ]

        subgraph PARALLEL [Parallel TaskGroups - max 5 concurrent]
            EXTRACT[extract_snapshot\nBigQueryInsertJobOperator\nWatermark-filtered scan to staging]
            DETECT[detect_delta\nBigQueryInsertJobOperator\nDynamic hash SQL - specific cols or all]
            RUNDQ[run_dq_rules\nPythonOperator\nLoad rules from dq_rules table\nExecute BQ SQL per rule]
            DQGATE{DQ Gate\nBranchOperator}
            MERGE[merge_to_target\nBigQueryInsertJobOperator\nSchema-introspected dynamic MERGE]
            QUARANTINE[quarantine_failed_rows\nBigQueryInsertJobOperator\nRemove and log bad rows]

            EXTRACT --> DETECT --> RUNDQ --> DQGATE
            DQGATE -- PASS --> MERGE
            DQGATE -- QUARANTINE --> QUARANTINE
            QUARANTINE --> MERGE
        end

        WATERMARK[advance_watermark\nPythonOperator\nOnly on success]
        AUDIT[write_audit_record\nBigQueryInsertJobOperator\nFull run details]
    end

    REPORT[write_dag_report\nTriggerRule ALL_DONE\nConsolidated summary - runs always]

    subgraph FWCONFIG [BQ Dataset - framework_config]
        R1[(table_registry)]
        R2[(dq_rules)]
        R3[(process_control)]
        R4[(run_audit)]
        R5[(dq_error_log)]
        R6[(quarantine_records)]
    end

    subgraph ALERTING [Alerting]
        CM[Cloud Monitoring\nCustom metrics per table]
        SL[Slack and PagerDuty\nOn DQ block or pipeline failure]
    end

    DISCOVER --> PARALLEL
    MERGE --> WATERMARK --> AUDIT --> REPORT

    DISCOVER -. reads .-> R1
    RUNDQ -. reads rules .-> R2
    WATERMARK -. updates .-> R3
    AUDIT -. inserts .-> R4
    RUNDQ -. writes errors .-> R5
    QUARANTINE -. writes rows .-> R6
    REPORT -. pushes metrics .-> CM
    CM -. alerts .-> SL
```

### Why This Pattern Wins

- **Composer** owns orchestration, retries, SLA alerts, parallelism control
- **Python** generates dynamic BQ SQL at runtime — no hardcoded column lists, works for any table schema
- **BQ** does all the heavy lifting — hash computation, delta joins, DQ checks, MERGE — at petabyte scale
- **Config tables** in BQ mean zero code changes to onboard a new table — just two SQL `INSERT` statements
- **Watermark only advances on success** — if anything fails, the next run reprocesses the same window

---

## Decision Matrix

### Legend

| Badge | Meaning |
|---|---|
| **BEST** | Ideal fit — recommended choice for this criteria |
| **GOOD** | Supported with standard setup |
| **POSSIBLE** | Works but with trade-offs or extra effort |
| **NO** | Not suited or requires significant workaround |

---

### Source Compatibility

| Criteria | Pure BQ SQL | Dataflow | Composer + BQ | Dataproc | Cloud Run Jobs | BQ + Dataplex |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **BigQuery source** — native BQ-to-BQ | `BEST` | `GOOD` | `BEST` | `POSSIBLE` | `GOOD` | `BEST` |
| **GCS / External files** — Parquet, CSV, Avro | `NO` | `BEST` | `POSSIBLE` | `BEST` | `GOOD` | `NO` |
| **External DB / API** — Cloud SQL, Spanner, REST | `NO` | `BEST` | `POSSIBLE` | `GOOD` | `BEST` | `NO` |

### Delta Detection

| Criteria | Pure BQ SQL | Dataflow | Composer + BQ | Dataproc | Cloud Run Jobs | BQ + Dataplex |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Key-based delta** — insert/update by PK | `BEST` | `GOOD` | `BEST` | `GOOD` | `GOOD` | `BEST` |
| **Hash-based delta** — MD5 row fingerprint | `BEST` | `GOOD` | `BEST` | `GOOD` | `GOOD` | `GOOD` |
| **Complex delta logic** — multi-col, nested, fuzzy | `POSSIBLE` | `BEST` | `GOOD` | `BEST` | `GOOD` | `POSSIBLE` |

### Data Quality

| Criteria | Pure BQ SQL | Dataflow | Composer + BQ | Dataproc | Cloud Run Jobs | BQ + Dataplex |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Simple SQL DQ rules** — null, range, uniqueness | `BEST` | `POSSIBLE` | `BEST` | `POSSIBLE` | `GOOD` | `BEST` |
| **Complex DQ logic** — cross-field, referential, ML | `NO` | `BEST` | `GOOD` | `BEST` | `GOOD` | `POSSIBLE` |
| **DQ audit trail** — governed, catalogued | `POSSIBLE` | `POSSIBLE` | `POSSIBLE` | `NO` | `NO` | `BEST` |
| **Dead letter / quarantine** — route failed rows | `POSSIBLE` | `BEST` | `GOOD` | `GOOD` | `GOOD` | `GOOD` |

### Scale and Performance

| Criteria | Pure BQ SQL | Dataflow | Composer + BQ | Dataproc | Cloud Run Jobs | BQ + Dataplex |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Very large scale — TB+** | `BEST` | `BEST` | `POSSIBLE` | `BEST` | `NO` | `BEST` |
| **Medium scale — GB range** | `BEST` | `GOOD` | `BEST` | `POSSIBLE` | `BEST` | `BEST` |

### Operations and Orchestration

| Criteria | Pure BQ SQL | Dataflow | Composer + BQ | Dataproc | Cloud Run Jobs | BQ + Dataplex |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Multi-step orchestration** — complex DAG | `NO` | `POSSIBLE` | `BEST` | `POSSIBLE` | `POSSIBLE` | `GOOD` |
| **Retry and SLA alerting** — auto-retry, monitoring | `POSSIBLE` | `GOOD` | `BEST` | `POSSIBLE` | `GOOD` | `GOOD` |
| **Data lineage tracking** — column-level provenance | `NO` | `POSSIBLE` | `POSSIBLE` | `NO` | `NO` | `BEST` |

### Cost and Complexity

| Criteria | Pure BQ SQL | Dataflow | Composer + BQ | Dataproc | Cloud Run Jobs | BQ + Dataplex |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Operational overhead** — infra to manage | `BEST` | `GOOD` | `POSSIBLE` | `NO` | `BEST` | `GOOD` |
| **Cost efficiency** — pay-per-query vs always-on | `BEST` | `POSSIBLE` | `POSSIBLE` | `NO` | `BEST` | `POSSIBLE` |
| **Implementation speed** — time to production | `BEST` | `POSSIBLE` | `GOOD` | `POSSIBLE` | `BEST` | `GOOD` |
| **Unit testability** — framework test support | `NO` | `GOOD` | `BEST` | `GOOD` | `BEST` | `POSSIBLE` |
| **Regulatory compliance** — GDPR, CCPA, audit | `NO` | `POSSIBLE` | `POSSIBLE` | `NO` | `NO` | `BEST` |

---

### Quick Recommendation by Scenario

| Scenario | Recommended Solution |
|---|---|
| BQ-to-BQ · Simple DQ · Fast delivery | **Pure BQ SQL + Scheduled Queries** |
| BQ-to-BQ · Enterprise audit trail + lineage | **BQ + Dataplex DQ + Composer** |
| External source · Complex transforms · TB+ scale | **Dataflow + Composer orchestration** |
| GCS files · Large data · Spark ecosystem | **Serverless Dataproc + Composer** |
| Medium scale · Custom Python logic · Low cost | **Cloud Run Jobs + BQ Storage Write API** |
| Multi-step DAG · Mixed sources · SLA monitoring | **Cloud Composer + BQ Operators + DQ Gate** |

---

*All architecture diagrams use [Mermaid](https://mermaid.js.org/) — renders natively on GitHub, GitLab, and VS Code.*
