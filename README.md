# WonderLink Backend

A TypeScript-based backend service that processes BigQuery analytics data and exposes APIs using tRPC.

## 🚀 Features

- tRPC HTTP server with TypeScript support
- BigQuery data processing capabilities
- PostgreSQL database integration using Drizzle ORM
- Environment configuration management
- CORS support for secure API access

## 📋 Prerequisites

- Node.js (Latest LTS version recommended)
- Yarn package manager
- PostgreSQL database
- Google Cloud Platform account with BigQuery access

## 🛠️ Environment Variables

Create a `.env` file in the root directory with the following variables:

```properties
HTTP_SERVER_PORT=
BIGQUERY_PROJECT_ID=
BIGQUERY_CREDENTIALS_JSON=
BIGQUERY_DATASET_NAME=
POSTGRESQL_DATABASE_URL=
```

## 🏗️ Project Structure

- `src/`
  - `index.ts` - Main application entry point
  - `trpc.ts` - tRPC server configuration
  - `scripts/` - Utility scripts
    - `process_bq_data.ts` - BigQuery data processing script
  - `db/` - Database related files and schemas

## 👥 Author

Doruk Özer (<dorukozer@protonmail.com>)
