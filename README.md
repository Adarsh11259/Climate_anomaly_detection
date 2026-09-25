# Climate Anomaly Detection Engine

A PySpark / Spark SQL / Delta Lake pipeline that flags weather stations currently showing statistically unusual temperature or precipitation readings, measured against each station's own historical baseline. Built on Databricks Free Edition using the medallion architecture (bronze → silver → gold).

## Data Source

[NOAA GHCN-Daily](https://registry.opendata.aws/noaa-ghcn/) (Global Historical Climatology Network - Daily) — a free, public dataset on AWS S3 with daily weather observations (temperature, precipitation, snowfall) from tens of thousands of stations worldwide, going back to 1763.

- Observations: `s3://noaa-ghcn-pds/csv/by_year/YYYY.csv`
- Station metadata: `s3://noaa-ghcn-pds/ghcnd-stations.txt` (fixed-width text)

## Architecture

All tables are managed Delta tables in a Unity Catalog schema (`climate_project`) on Databricks Free Edition.

| Layer  | Notebook              | Output                                   | What happens                                                                                                                                                                                 |
| ------ | --------------------- | ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Bronze | `02_bronze_ingest`    | `bronze_observations`                    | Raw CSVs (2016–2026) read with an explicit schema and loaded as-is — ~391.5M rows                                                                                                            |
| Silver | `03_silver_transform` | `silver_observations`, `silver_stations` | Quality-flag filtering, type casting, unit rescaling (raw values ÷10 → real-world units); station metadata parsed from the fixed-width file — ~210M clean observation rows, ~132.5K stations |
| Gold   | `04_gold_analytics`   | `gold_baseline`, `gold_anomalies`        | Rolling ±7-day historical baselines per station/day-of-year, z-score anomaly detection, station metadata join                                                                                |
| Bonus  | `05_station_scd_demo` | —                                        | Delta `MERGE INTO` demo simulating a station metadata update (re-survey + new station) as a slowly-changing dimension                                                                        |

`01_ghcn_exploration` is early exploratory/reference work, not part of the production pipeline.

## How Anomaly Detection Works

1. **Baseline (historical years)**: for each station, element (TMAX/TMIN/PRCP), and day-of-year, compute the mean and standard deviation of readings pooled across a ±7-day window over the historical years — this smooths out day-to-day noise while still capturing seasonal norms.
2. **Data quality gate**: baselines built from fewer than 30 pooled historical observations are discarded. During development, thin baselines (as few as 15 observations) produced near-zero standard deviations, which inflated some z-scores into the thousands and even produced physically impossible baseline values (e.g., a tropical station's baseline showing sub-zero temperatures). Filtering on `rolling_count >= 30` fixed this at the source.
3. **Scoring (current year)**: each current-year reading is joined to its station/day-of-year baseline, and a z-score is computed as `(value - baseline_mean) / baseline_stddev`, using `try_divide` to safely handle the rare case of zero variance.
4. **Flagging**: readings beyond 3 standard deviations from baseline are flagged as anomalies and joined against station metadata (name, state, lat/lon) for interpretability.

### A note on precipitation

Flagged anomalies skew heavily toward PRCP over temperature (roughly 195K PRCP vs. ~26K TMAX and ~23K TMIN out of ~244K total anomalies). This is expected, not a bug: precipitation is zero-inflated and highly skewed — most days have 0mm of rain at a given station, so the baseline standard deviation is naturally tiny, and any real rain event produces a large z-score. Temperature, by contrast, varies more smoothly day to day.

## Delta Lake in Practice

- **ACID writes** for every bronze/silver/gold table.
- **`MERGE INTO`** (`05_station_scd_demo`) demonstrates upserting station metadata changes — updating a re-surveyed elevation and inserting a new station — in a single atomic, idempotent operation, safe to re-run without creating duplicates. This is a simulated update batch (the NOAA station file is a static snapshot), documented as such.

## Tech Stack

PySpark, Spark SQL, Delta Lake, Databricks Free Edition (Unity Catalog), AWS S3 (public dataset access).

## Notebooks

1. `01_ghcn_exploration.ipynb` — exploratory reference work
2. `02_bronze_ingest.ipynb` — raw ingestion
3. `03_silver_transform.ipynb` — cleaning and standardization
4. `04_gold_analytics.ipynb` — rolling baselines and anomaly scoring
5. `05_station_scd_demo.ipynb` — Delta `MERGE INTO` / SCD demo
