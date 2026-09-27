# 🌐 FastAPI HDFS Gateway

A production-ready asynchronous REST API and Web Gateway for **Apache Hadoop HDFS (Hadoop Distributed File System)**.

This gateway provides a high-performance HTTP interface to an existing Hadoop HDFS installation, exposing endpoints to upload, stream-download, list, create directories, and delete files without modifying the Hadoop cluster infrastructure or requiring Hadoop CLI client binaries.

---

## 🎯 System Architecture

The gateway is built on macOS, containerized with Docker, version-controlled via GitHub, and deployed directly onto the Linux host running Apache Hadoop.

```mermaid
flowchart TD
    subgraph Client Layer
        Browser["🖥️ Web Browser (Management UI)"]
        ClientApp["⚡ External Services & HTTP Clients"]
    end

    subgraph Gateway Layer ["🚪 FastAPI Gateway (Port 5005)"]
        FastAPI["FastAPI App (app/main.py)"]
        Endpoints["REST Endpoints (/api/v1)"]
        Service["HDFSService (app/services/hdfs_service.py)"]
        Config["Configuration (Pydantic / .env)"]
    end

    subgraph Hadoop Layer ["🐘 Existing Hadoop Infrastructure (Linux Host)"]
        NameNode["Hadoop NameNode (WebHDFS Port 9870)"]
        DataNode["Hadoop DataNodes (HTTP Block Transfer Port 9864)"]
    end

    Browser -->|HTTP Requests| FastAPI
    ClientApp -->|REST / JSON| FastAPI
    FastAPI --> Endpoints
    Endpoints --> Service
    Service --> Config

    %% WebHDFS Operations
    Service -->|1. PUT op=CREATE (Init)| NameNode
    NameNode -->|307 Redirect (Location: DataNode)| Service
    Service -->|2. Stream PUT Payload| DataNode
    Service -->|GET op=OPEN (Stream Download)| DataNode
    Service -->|GET op=LISTSTATUS / PUT op=MKDIRS / DELETE| NameNode
```

### 🔑 Architecture Highlights
- **External Dependency Model:** Hadoop HDFS is treated as existing backend infrastructure. The gateway modifies zero Hadoop configs.
- **Pure WebHDFS Protocol:** Communicates via standard Hadoop WebHDFS v1 REST specifications, adhering to the 2-step `307 Temporary Redirect` upload lifecycle.
- **Zero-Footprint Streaming:** Direct asynchronous chunk streaming for uploads and downloads prevents memory spikes when transferring large files.
- **Security & Sanitization:** All user-supplied HDFS paths are normalized and protected against traversal attacks (`/..`).

---

## 📂 Project Structure

```
.
├── app/
│   ├── __init__.py
│   ├── main.py                     # FastAPI application entrypoint, CORS & templates
│   ├── api/
│   │   ├── __init__.py
│   │   └── endpoints.py            # REST API routes (/api/v1/health, files/list, upload, etc.)
│   ├── core/
│   │   ├── __init__.py
│   │   └── config.py               # Pydantic Settings & environment variable configuration
│   ├── services/
│   │   ├── __init__.py
│   │   └── hdfs_service.py         # Async WebHDFS client communicating with Hadoop NameNode/DataNode
│   ├── templates/
│   │   └── index.html              # Management UI dashboard
│   └── static/                     # Static UI assets
├── tests/
│   ├── __init__.py
│   └── test_health.py              # Pytest async test suite (ASGITransport)
├── .env.example                    # Template environment variables
├── .gitignore                      # Git ignore rules for Python & environment files
├── Dockerfile                      # Production Python 3.12 container image
├── docker-compose.yml              # Docker Compose deployment definition
├── Makefile                        # Automation shortcuts (run, dev, test, docker-build)
├── requirements.txt                # Production Python dependencies
└── requirements-dev.txt            # Development & testing dependencies
```

---

## 📡 REST API Reference

All REST endpoints are grouped under `/api/v1`. Interactive OpenAPI documentation is accessible at [`/docs`](http://localhost:5005/docs).

### 1. Health & Cluster Connectivity
- **`GET /api/v1/health`**
- Verifies gateway operation and checks active connectivity to the Hadoop NameNode.

### 2. Directory Listing
- **`GET /api/v1/files/list?path=/`**
- **Query Parameter:** `path` (default: `/`)
- **Sample Response:**
  ```json
  {
    "path": "/data",
    "count": 2,
    "items": [
      {
        "name": "logs",
        "path": "/data/logs",
        "type": "DIRECTORY",
        "length": 0,
        "permission": "755",
        "modificationTime": 1711540000000
      },
      {
        "name": "dataset.csv",
        "path": "/data/dataset.csv",
        "type": "FILE",
        "length": 1048576,
        "permission": "644",
        "modificationTime": 1711540200000
      }
    ]
  }
  ```

### 3. File Upload
- **`POST /api/v1/files/upload`**
- **Form Data:**
  - `file`: Multipart binary file
  - `destination_dir`: Target HDFS directory path (e.g. `/data`)
  - `overwrite`: `true` or `false`
- **Response:**
  ```json
  {
    "message": "File uploaded successfully",
    "path": "/data/dataset.csv"
  }
  ```

### 4. File Download
- **`GET /api/v1/files/download?path=/data/dataset.csv`**
- Streams the file directly from Hadoop HDFS to the client.

### 5. Directory Creation
- **`POST /api/v1/files/mkdir?path=/data/new_folder`**

### 6. File & Directory Deletion
- **`DELETE /api/v1/files/delete?path=/data/dataset.csv&recursive=true`**

---

## ⚙️ Environment Configuration

Configuration is managed via environment variables and loaded through Pydantic Settings.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `APP_NAME` | `HDFS Gateway API` | Application name in UI and Swagger docs |
| `PORT` | `5005` | Gateway HTTP listen port |
| `HOST` | `0.0.0.0` | Network interface binding |
| `HDFS_NAMENODE_URL` | `http://localhost:9870` | URL to the Hadoop NameNode WebHDFS service |
| `HDFS_USER` | `hadoop` | HDFS username executing filesystem operations |
| `HDFS_DEFAULT_DIR` | `/` | Default root directory for file browsing |
| `HDFS_TIMEOUT_SECONDS` | `30.0` | HTTP request timeout for HDFS operations |

---

## 🚀 Deployment & Operations

### 1. Local Development
```bash
# Install dependencies
pip install -r requirements-dev.txt

# Run server on port 5005 with auto-reload
make dev
```
Open [http://localhost:5005](http://localhost:5005) for the UI or [http://localhost:5005/docs](http://localhost:5005/docs) for the Swagger API docs.

### 2. Testing & Code Quality
```bash
# Run test suite
pytest -v

# Run linting
ruff check .
```

### 3. Docker Deployment
```bash
# Build Docker image
docker build -t hdfs-gateway:latest .

# Run container with environment configuration
docker run -d -p 5005:5005 --env-file .env hdfs-gateway:latest
```
