# Riverline Backend

This repository contains the backend services for the Riverline project, focusing on a robust data ingestion pipeline and Next Best Action (NBA) analytics.

## 🚀 Getting Started

### Prerequisites

*   Python 3.9+
*   Java (for PySpark)
*   Docker (for local database setup, if not using cloud instances)
*   Poetry (recommended for dependency management)

### Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Arvind-puthucode/Riverline-backend.git
    cd Riverline-backend/
    ```

2.  **Install Dependencies:**
    It's recommended to use `poetry` for dependency management.

    ```bash
    # Install poetry if you haven't already
    pip install poetry

    # Install project dependencies
    poetry install
    ```
    If you prefer `pip`:
    ```bash
    pip install -r requirements.txt # You might need to generate this first if not present
    ```

3.  **Environment Configuration (`.env` file):**
    Create a `.env` file in the `riverline/backend/` directory (one level up from `riverline_backend`) with your database credentials. This file is automatically loaded by the application.

    Example `.env` content:
    ```dotenv
    SUPABASE_URL="https://your-supabase-url.supabase.co"
    SUPABASE_KEY="your-supabase-anon-key"
    CLICKHOUSE_HOST="your-clickhouse-host"
    CLICKHOUSE_PORT="your-clickhouse-port" # e.g., 8443 for HTTPS
    CLICKHOUSE_USER="your-clickhouse-user"
    CLICKHOUSE_PASSWORD="your-clickhouse-password"

    # Default data engine (can be overridden via environment variable)
    DATA_ENGINE=pandas # or "spark"
    ```

## 📊 Data Pipeline

The data pipeline processes CSV data, normalizes it, performs quality checks, and ingests it into either Supabase (PostgreSQL) or ClickHouse.

### Database Choice: Why Two?

The pipeline supports two different database backends: Supabase (PostgreSQL) and ClickHouse. This design allows for a direct comparison of their performance and scalability characteristics for data ingestion and analytical workloads. PostgreSQL is a robust relational database, while ClickHouse is an analytical column-oriented database optimized for high-speed data processing.

### Running the Pipeline

You can specify the database target and the data processing engine (Pandas or Spark).

**Command Structure:**

```bash
python main.py --action run_pipeline --db <database_target>
```

**Parameters:**

*   `--action run_pipeline`: Specifies that the data ingestion pipeline should be executed.
*   `--db <database_target>`:
    *   `supabase`: Ingests data into the configured Supabase (PostgreSQL) instance.
    *   `clickhouse`: Ingests data into the configured ClickHouse instance.

**Data Engine Selection (Environment Variable):**

The `DATA_ENGINE` environment variable determines which processing engine is used.

*   `DATA_ENGINE=pandas`: Uses the Pandas-based data engine (default if not set). Suitable for smaller datasets or local development.
*   `DATA_ENGINE=spark`: Uses the PySpark-based data engine. Recommended for larger datasets and distributed processing.

**Examples:**

*   **Run pipeline with Pandas engine and Supabase:**
    ```bash
    DATA_ENGINE=pandas python main.py --action run_pipeline --db supabase
    ```

*   **Run pipeline with Spark engine and ClickHouse:**
    ```bash
    DATA_ENGINE=spark python main.py --action run_pipeline --db clickhouse
    ```

## 📈 Next Best Action (NBA)

The project includes functionalities for processing and generating Next Best Action (NBA) predictions, leveraging both rule-based logic and LLM enhancement.

### NBA Modes

There are two primary modes for running the NBA engine:

1.  **Batch Processing (for Evaluation)**: Designed for processing a large number of customer profiles and exporting predictions for analysis.
2.  **API Server (for Production Use)**: Provides a FastAPI endpoint for real-time, single-customer NBA predictions.

### Running NBA Processing

This action processes raw interactions to generate aggregated conversation threads and customer profiles, storing them in the selected database. This step is a prerequisite for running NBA predictions.

```bash
python main.py --action process_nba_data --db <database_target>
```

### Running NBA Predictions (Batch Mode)

This action fetches processed customer data and runs the NBA prediction engine in batch mode. It's suitable for evaluation and generating predictions for a specified number of customers.

**Command Structure:**

```bash
python main.py --action run_nba_predictions --db <database_target> [--customers <limit>]
```

**Parameters:**

*   `--action run_nba_predictions`: Specifies the batch NBA prediction mode.
*   `--db <database_target>`:
    *   `supabase`: Uses the Supabase (PostgreSQL) database as the data source.
    *   `clickhouse`: Uses the ClickHouse database as the data source.
*   `--customers <limit>` (Optional): Limits the number of customer profiles to process. If omitted, all available customer profiles will be processed. This is useful for controlling the scope of evaluation runs.

**Example:**

*   **Process 1000 customers for evaluation using ClickHouse:**
    ```bash
    python main.py --action run_nba_predictions --db clickhouse --customers 1000
    ```

### Running NBA API Server

This action starts a FastAPI server that exposes an endpoint for real-time NBA predictions for individual customers. This is intended for production deployment.

**Command Structure:**

```bash
python main.py --action run_nba_api --db <database_target>
```

**Parameters:**

*   `--action run_nba_api`: Specifies that the FastAPI NBA prediction server should be started.
*   `--db <database_target>`:
    *   `supabase`: Configures the API to fetch data from Supabase.
    *   `clickhouse`: Configures the API to fetch data from ClickHouse.

**Example:**

*   **Start NBA API server using Supabase:**
    ```bash
    python main.py --action run_nba_api --db supabase
    ```

*   **Example API Call (after server is running):**
    ```bash
    curl -X POST "http://localhost:8000/predict_nba" \
         -H "Content-Type: application/json" \
         -d '{"customer_id": "twitter_user_123"}'
    ```

## 🧪 Evaluation

To run the evaluation script:

```bash
python main.py --action run_evaluation --db <database_target>
```
