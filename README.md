# ACME Delivery Services Analytics Pipeline

This document provides detailed instructions on how to set up and use the ACME Delivery Services Analytics Pipeline. This tool captures real-time data changes from the operational database, transforms them for analytical purposes, and offers a command-line interface to export business insights.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Running the Infrastructure](#running-the-infrastructure)
- [Starting the CDC Pipeline](#starting-the-cdc-pipeline)
- [Using the CLI Tool](#using-the-cli-tool)
- [Available Queries](#available-queries)
- [Architecture Overview](#architecture-overview)
- [Troubleshooting](#troubleshooting)

## Prerequisites

Before getting started, ensure you have the following installed on your system:

- [Docker](https://www.docker.com/products/docker-desktop/) and [Docker Compose](https://docs.docker.com/compose/install/)
- [Python 3.8+](https://www.python.org/downloads/)
- [Git](https://git-scm.com/downloads)

## Installation

1. Clone the repository:

```bash
git clone https://github.com/mauro-rodrigues/deel-data-engineering-task.git
cd deel-data-engineering-task
```

2. Create a Python virtual environment and activate it:

```bash
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

3. Install the required Python packages:

```bash
pip install -r requirements.txt
```

## Running the Infrastructure

The project uses Docker Compose to set up and run all necessary infrastructure components:

1. Start the Docker containers:

```bash
docker-compose up -d
```

This command will start:
- The source transactions database (PostgreSQL)
- Kafka and Zookeeper for streaming data
- Debezium Connect for Change Data Capture
- The analytics database (PostgreSQL)

2. Verify that all containers are running:

```bash
docker-compose ps
```

All services should be in the "Up" state. Wait a few moments for all services to initialize properly.

## Starting the CDC Pipeline

Once the infrastructure is up and running, you need to start the CDC (Change Data Capture) pipeline:

```bash
python src/main.py
```

This script will:
1. Initialize the analytics database schema
2. Set up the Debezium connectors to capture changes from the source database
3. Start the Kafka consumer to process change events
4. Transform and load the data into the analytics database

You should see log messages indicating that the pipeline is successfully capturing and processing data changes.

> Note: Keep this process running in a terminal window while you want to capture data changes. You can use tools like `tmux`, `screen`, or `nohup` to keep the process running in the background.

## Using the CLI Tool

The analytics pipeline comes with a command-line interface (CLI) tool that allows you to export business insights to CSV files. The tool is located at `src/export/cli.py`.

To use the CLI tool:

```bash
python src/export/cli.py [query]
```

Where `[query]` is one of the available query options (see [Available Queries](#available-queries)).

Each query will generate a CSV file in the `output` directory with a timestamp in the filename.

### Example:

```bash
python src/export/cli.py orders
```

This will export data on open orders by delivery date and status to a file like `output/orders_by_delivery_status_20230316_123456.csv`.

## Available Queries

The CLI tool supports the following query options:

1. `orders` - Number of open orders by delivery date and status
   ```bash
   python src/export/cli.py orders
   ```

2. `top-dates` - Top 3 delivery dates with more open orders
   ```bash
   python src/export/cli.py top-dates
   ```

3. `pending-items` - Number of open pending items by product ID
   ```bash
   python src/export/cli.py pending-items
   ```

4. `top-customers` - Top 3 customers with more pending orders
   ```bash
   python src/export/cli.py top-customers
   ```

## Architecture Overview

The analytics pipeline consists of the following components:

1. **Source Database**: PostgreSQL database containing operational data (customers, products, orders, order_items).

2. **Debezium & Kafka**: Captures change events from the source database and publishes them to Kafka topics.

3. **CDC Consumer**: Consumes change events from Kafka and processes them.

4. **Analytics Database**: Stores transformed data optimized for analytical queries.

5. **CLI Tool**: Provides a command-line interface to export business insights.

## Troubleshooting

### Common Issues

1. **Docker containers not starting**:
   - Check Docker logs: `docker-compose logs`
   - Ensure ports 5432, 5433, 9092, and 8083 are not already in use

2. **CDC pipeline not capturing changes**:
   - Check Debezium Connect logs: `docker-compose logs debezium`
   - Verify that the source database has logical replication enabled

3. **CLI tool not connecting to the analytics database**:
   - Verify that the analytics database is running: `docker-compose ps analytics-db`
   - Check the connection parameters in `src/transformations/analytics_db.py`

4. **No data in exported CSV files**:
   - Ensure that the CDC pipeline has been running long enough to capture data
   - Check the logs of the CDC pipeline for any errors

### Restarting the Pipeline

If you need to restart the entire pipeline:

1. Stop and remove the containers:
   ```bash
   docker-compose down
   ```

2. Start the containers again:
   ```bash
   docker-compose up -d
   ```

3. Restart the CDC pipeline:
   ```bash
   python src/main.py
   ```

### Clearing Data

To clear the data and start fresh:

1. Stop the containers:
   ```bash
   docker-compose down
   ```

2. Remove the volumes:
   ```bash
   docker-compose down -v
   ```

3. Start the containers again:
   ```bash
   docker-compose up -d
   ```

4. Restart the CDC pipeline:
   ```bash
   python src/main.py
   ``` 
