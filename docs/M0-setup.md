# M0: Set up your machine and meet the data

**Goal:** by the end of M0 you have Docker running with enough resources, this repo on your SSD, the Olist dataset downloaded, and a first look at the data that answers the questions every later milestone depends on.

Time: about 1 hour.

---

## Step 1. Give Colima enough resources (and access to your SSD)

You already have [Colima](https://github.com/abiosoft/colima) installed. Colima runs a small Linux VM on your Mac, and Docker containers run inside that VM. Two things matter for us:

1. **Memory and CPU.** The full stack (Trino, Spark, Airflow, MinIO, Postgres, catalog) needs about 7 to 8 GB. Your Mac has 16 GB, so we give Colima 8 GB and leave 8 GB for macOS, your IDE and the browser.
2. **Mounts.** By default Colima only shares your home folder (`~`) with the VM. This project lives on `/Volumes/SandiskSSD`, so without an extra mount every container would see an **empty folder** where your code should be. This is the most common "why is my DAG folder empty?" problem on Macs.

```bash
colima stop   # only if it is already running

colima start \
  --cpu 4 \
  --memory 8 \
  --disk 60 \
  --vm-type vz \
  --mount-type virtiofs \
  --mount /Volumes/SandiskSSD:w \
  --mount $HOME:w
```

Why each flag:

| Flag | Why |
| --- | --- |
| `--cpu 4 --memory 8` | Trino and Spark are JVM programs; with less memory they crash with `OutOfMemoryError` or get killed silently |
| `--disk 60` | Docker images for this stack are about 10 GB; table data and logs grow over time |
| `--vm-type vz` | Apple's native Virtualization.framework: faster than QEMU on Apple Silicon |
| `--mount-type virtiofs` | Fastest file sharing between macOS and the VM (needs `vz`) |
| `--mount /Volumes/SandiskSSD:w` | Lets containers read and write the project folder on your SSD |

Check it: `colima list` should show 4 CPUs and 8 GiB.

Note: Colima keeps its VM disk under `~/.colima` on your internal drive. That is fine; only the project files and dataset live on the SSD.

## Step 2. Clone the repo onto the SSD

```bash
cd /Volumes/SandiskSSD/Developer/Data_Engineering
git clone https://github.com/<your-username>/shoplake-lakehouse.git shoplake
cd shoplake
cp .env.example .env
```

## Step 3. Run the machine check

```bash
make check
```

You want every line to say `[ok]`. If the bind-mount line warns, go back to Step 1 and check the `--mount` flag.

## Step 4. Download the dataset

You already have a `~/.kaggle` folder, so the Kaggle API token is probably set up. If not: kaggle.com, Settings, API, "Create New Token", and save `kaggle.json` to `~/.kaggle/` ([Kaggle API docs](https://www.kaggle.com/docs/api)).

```bash
make venv   # local Python env with kaggle + duckdb
make data   # downloads ~45 MB zip, unzips 9 CSVs into data/raw
```

Run `make data` a second time. It prints "nothing to do". That is **idempotency**: running a step twice gives the same result as running it once. Every pipeline step we build will have this property, because Airflow retries and backfills re-run steps all the time. (It is the same idea as an idempotent Kafka consumer that can safely see a message twice.)

`data/raw` is in `.gitignore`: data does not go in Git. It is big, and anyone can re-create it with `make data`.

## Step 5. Look at the data before designing anything

```bash
make explore
```

The script uses [DuckDB](https://duckdb.org/docs/data/csv/overview) (which you used in the ShopBack take-home) to answer eight questions. Write your answers down; we use each of them later:

| # | Question | Where it matters |
| --- | --- | --- |
| 1 | How many rows per table? | Sizing Spark and Trino memory (M1, M2) |
| 2 | Is `order_id` unique in orders? | Primary key and the `MERGE` condition (M3) |
| 3 | Is `(order_id, order_item_id)` unique? | The **grain** of `fact_order_items` (M5) |
| 4 | First and last purchase date? | The **watermark** column for incremental loads (M3) |
| 5 | Orders per month? | Partition by day or by month? (M2, partition evolution in M3) |
| 6 | Which order statuses exist? | Rows change after they land, so plain appends are not enough (M3) |
| 7 | `customer_id` vs `customer_unique_id`? | Which key `dim_customer` uses and how SCD2 tracks a person (M5) |
| 8 | Delivered orders with no delivery date? | Data quality tests (M4) |

Hint for #7: Olist creates a new `customer_id` for every order, and `customer_unique_id` is the real person. Think about which one a "repeat customer rate" must use.

## Done when

- [ ] `make check` shows all `[ok]`
- [ ] `data/raw` has 9 CSV files
- [ ] You have answers to the 8 questions above

Then tell me your answers (or any surprises) and we start **M1: MinIO + Iceberg REST catalog + Trino**.

## Read more (optional)

- Colima README: https://github.com/abiosoft/colima
- Docker Compose overview: https://docs.docker.com/compose/
- The dataset page, with its schema diagram: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
