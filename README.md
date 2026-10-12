# Lakeflow Declarative Pipelines for SmartFactory

Databricks **Lakeflow Declarative Pipelines** (DLT) project that builds a
Bronze → Silver lakehouse for a *SmartFactory* dataset. Raw files land in a Unity
Catalog volume and are ingested with Auto Loader (`cloudFiles`).

```
/Volumes/dlt_lakehouse/default/raw/  →  BRONZE  →  SILVER
```

Everything lives in the catalog `dlt_lakehouse` (schemas `bronze` and `silver`).

## How it works

**Bronze** ingests raw files (one file per source) and keeps the data as-is:

| Source                          | Format    | Table                     |
| ------------------------------- | --------- | ------------------------- |
| `events/*.pb`                   | Protobuf  | `events_cdc_bronze`       |
| `machines/`                     | CSV       | `machines_cdc_bronze`     |
| `telemetry/*.avro`              | Avro      | `telemetry_cdc_bronze`    |
| `work_orders/*.jsonl`           | JSONL     | `work_orders_cdc_bronze`  |
| `quality/*.json`                | JSON      | `quality_cdc_bronze`      |
| `maintenance/*.orc`             | ORC       | `maintenance_cdc_bronze`  |

**Silver** decodes and models that data:

| Table                          | What it does                                         |
| ------------------------------ | ---------------------------------------------------- |
| `silver.events_cdc_silver`     | Decodes Protobuf events; drops bad rows              |
| `silver.maintance_cdc_silver`  | Parses the `window` JSON into columns                |
| `silver.latest_machine_maintance` | Latest maintenance `actual_start` per machine     |
| `silver.machine_maintenance_cdc`  | Current-state table (SCD Type 1) per machine      |

## Layout

```
SmartFactory/
├── get_spark.py                      # Shared Spark session
└── transformations/
    ├── 001_bronze/                   # Ingestion (one file per source)
    └── 002_silver/                   # Decoding & modelling
events.desc / events_pb2.py           # Protobuf descriptor for event decoding
```

## License

[MIT](LICENSE) © 2026 Mikeias Oliveira
