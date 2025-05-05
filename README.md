# Real Estate Data Pipeline & LLM Agent Application

## 📖 Overview

This repository implements a **complete end-to-end pipeline** for collecting, processing, and serving real estate data in Hanoi, Vietnam, focusing on four property types: *street-facing houses*, *alley houses*, *apartments*, and *villas*. The pipeline integrates data crawling, processing, storage, and search capabilities, culminating in a user-facing application powered by a sophisticated LLM-based agent system. The application features a chatbot for interactive Q&A and a dashboard for data visualization.

---

## 🏗️ Project Architecture

The repository is structured into modular components, each handling a specific part of the pipeline or application:

- **crawler/**: A Scrapy-based crawler to collect real estate listings. Exposed via a FastAPI endpoint for other services to trigger data collection.
- **kafka/**: A Kafka cluster (with Zookeeper and 3 brokers) to manage data streaming. It includes topics for each property type to receive data from the crawler.
- **cloud/**: A Kafka consumer that fetches data from Kafka topics and uploads it to Google Cloud Storage (GCS) for persistent storage.
- **airflow/**: An Apache Airflow setup with a DAG to schedule data collection (at 12 PM and 12 AM daily). It triggers the crawler, then submits a Spark job to process data from GCS and index it into Elasticsearch.
- **spark/**: A utility module for testing data processing and connectivity between GCS and Elasticsearch.
- **elasticsearch/**: An Elasticsearch cluster with Kibana for indexing, searching, and visualizing real estate data.
- **agent-backend/**: The backend for the LLM agent system, powered by a prompt-chaining pattern with five agents:
  - **Search Database Agent**: Queries Elasticsearch for relevant listings based on user input.
  - **Search Web Agent**: Searches the web for additional insights (e.g., market trends) not available in the database.
  - **Manager Agent**: Decides which of the above agents to delegate tasks to.
  - **Judge Agent**: Evaluates responses from the Search agents and determines if further searching is needed.
  - **Writer Agent**: Refines and enriches the final response with additional insights.
  This module also includes Elasticsearch queries for data visualization.
- **frontend/**: A user interface with two main features:
  - **Chatbot**: Allows users to interact with the LLM agents for real estate Q&A.
  - **Dashboard**: Visualizes data with charts and graphs using prebuilt Elasticsearch queries.

---

## 🚀 Getting Started

### Prerequisites

- Docker and Docker Compose installed on your machine.
- A Google Cloud Storage account and credentials for GCS integration.
- Basic familiarity with the technologies used (Kafka, Elasticsearch, Airflow, etc.).

### Running the Data Pipeline

The pipeline components must be started in the following order to ensure proper dependency resolution:

1. **Kafka Cluster**:
   ```bash
   cd kafka
   docker-compose up --build
   ```

2. **Elasticsearch and Kibana**:
   ```bash
   cd elasticsearch
   docker-compose up --build
   ```

3. **Cloud Consumer (Kafka to GCS)**:
   ```bash
   cd cloud
   docker-compose up --build
   ```

4. **Crawler**:
   ```bash
   cd crawler
   docker-compose up --build
   ```

5. **Airflow Scheduler**:
   ```bash
   cd airflow
   docker-compose up --build
   ```

### Running the Application

Once the pipeline is up and running, you can start the application services:

1. **Backend (Agent System)**:
   ```bash
   cd agent-bds
   docker-compose up --build
   ```

2. **Frontend (Chatbot and Dashboard)**:
   ```bash
   cd frontend
   docker-compose up --build
   ```

---

## 📊 Features

- **Automated Data Collection**: Scrapy-based crawler collects real estate listings twice daily, scheduled via Airflow.
- **Scalable Data Processing**: Kafka and Spark ensure efficient data streaming and processing.
- **Search and Insights**: Elasticsearch and Kibana enable fast querying and visualization of real estate data.
- **Intelligent LLM Agents**: A multi-agent system provides rich, context-aware responses to user queries, blending database search with web insights.
- **User-Friendly Interface**: A frontend with a chatbot for conversational queries and a dashboard for data visualization.

---

## 🛠️ Technologies Used

- **Crawler**: Scrapy, FastAPI
- **Streaming**: Apache Kafka, Zookeeper
- **Storage**: Google Cloud Storage
- **Scheduling**: Apache Airflow
- **Processing**: Apache Spark
- **Search**: Elasticsearch, Kibana
- **Backend**: Python (LLM agents with prompt chaining)
- **Frontend**: Custom UI (framework not specified)


---

## 📝 Notes

- The `spark/` directory is for testing purposes only and is not part of the main pipeline.
- Ensure proper configuration of GCS credentials and Elasticsearch settings before running the pipeline.
- The Airflow DAG runs at 12 PM and 12 AM daily—adjust the schedule as needed.

