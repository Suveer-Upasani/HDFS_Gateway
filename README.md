# HDFS Gateway

> A lightweight FastAPI web gateway for managing files stored in Apache Hadoop HDFS.

HDFS Gateway bridges modern web interfaces with distributed big data storage, providing a clean, responsive developer console and RESTful API on top of Apache Hadoop's Distributed File System (HDFS).

---

## 🏛️ Architecture

```text
       ┌───────────────────────────────┐
       │   Browser / Client Interface  │
       │    (HTML5 / CSS / Vanilla JS) │
       └──────────────┬────────────────┘
                      │ HTTP / REST
                      ▼
       ┌───────────────────────────────┐
       │     FastAPI Web Gateway       │
       │  (Jinja2 Templates + API Core)│
       └──────────────┬────────────────┘
                      │ Safe Subprocess CLI
                      ▼
       ┌───────────────────────────────┐
       │      Apache Hadoop HDFS       │
       │   (NameNode & DataNodes / CLI)│
       └───────────────────────────────┘
```

---

## ✨ Features

- **Developer-Oriented Dashboard**: Live HDFS connectivity monitoring, storage capacity overview, and recent files.
- **Full File Manager**: Search, list, download, and delete HDFS files directly from your browser.
- **Drag & Drop Uploads**: Seamless asynchronous file uploads with client-side validation and live progress states.
- **Safe HDFS Integration**: Interacts directly with the `hdfs dfs` CLI via secure Python subprocess calls without requiring unstable external bindings.
- **Resilient Fallback**: Designed to run seamlessly in decoupled environments (e.g. developed on macOS, deployed on Kali Linux VM with Hadoop) without crashing when HDFS is unreachable.
- **Path Traversal Protection**: Filename sanitization strictly rejects directory traversal attacks (e.g., `../../etc/passwd`).
- **RESTful API & Swagger Docs**: Fully documented endpoints with automatic OpenAPI generation at `/docs`.

---

## 📋 Requirements

- **Python 3.12+**
- **Apache Hadoop** (Hadoop 3.x+ with HDFS running on host or VM)
- **HDFS CLI** (`hdfs` command accessible in `$PATH` or via `HDFS_BIN`)

---

## 🚀 Quickstart & Installation

### 1. Clone Repository & Setup Virtual Environment

```bash
git clone https://github.com/Suveer-Upasani/hdfs-gateway.git
cd hdfs-gateway

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Environment Configuration

Copy the example configuration:

```bash
cp .env.example .env
```

Configurable variables in `.env`:
| Parameter | Default Value | Description |
| :--- | :--- | :--- |
| `HDFS_URI` | `hdfs://localhost:9000` | Target HDFS NameNode URI |
| `HDFS_UPLOAD_PATH`| `/user/suveer/hdfs-gateway/uploads` | Target directory inside HDFS |
| `MAX_UPLOAD_SIZE` | `52428800` (50 MB) | Max file upload limit in bytes |
| `HDFS_BIN` | `hdfs` | HDFS binary command or absolute path |

### 3. Initialize HDFS Upload Folder

On your Hadoop cluster / VM (e.g. Kali Linux):

```bash
hdfs dfs -mkdir -p /user/suveer/hdfs-gateway/uploads
```

### 4. Start the Application

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

- **Web UI**: [http://localhost:8000](http://localhost:8000)
- **Interactive Swagger Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc API Spec**: [http://localhost:8000/redoc](http://localhost:8000/redoc)

---

## 🌐 Web Routes & Navigation

| Route | View | Description |
| :--- | :--- | :--- |
| `GET /` | Redirect | Redirects to `/dashboard` |
| `GET /dashboard` | `dashboard.html` | Status metrics, cluster health, and recent files |
| `GET /files` | `files.html` | Comprehensive file browser with search and actions |
| `GET /upload` | `upload.html` | Drag-and-drop file uploader |

---

## 🔌 REST API Reference

All API routes return clean JSON payloads with appropriate HTTP status codes (`200`, `400`, `404`, `413`, `500`, `503`).

### Health Check
```http
GET /api/health
```
```json
{
  "status": "ok",
  "api": "Online",
  "hdfs": "Connected",
  "hdfs_connected": true,
  "upload_path": "/user/suveer/hdfs-gateway/uploads"
}
```

### List Files
```http
GET /api/files
```
```json
{
  "success": true,
  "count": 2,
  "files": [
    {
      "name": "test.txt",
      "size": 24,
      "size_formatted": "24 B",
      "path": "/user/suveer/hdfs-gateway/uploads/test.txt",
      "modified": "2026-09-26 18:45",
      "is_dir": false,
      "owner": "suveer"
    }
  ],
  "upload_path": "/user/suveer/hdfs-gateway/uploads"
}
```

### Upload File
```http
POST /api/upload
Content-Type: multipart/form-data
```
```json
{
  "success": true,
  "filename": "dataset.csv",
  "path": "/user/suveer/hdfs-gateway/uploads/dataset.csv",
  "size": 2202009,
  "size_formatted": "2.1 MB",
  "message": "Successfully uploaded 'dataset.csv' to HDFS."
}
```

### Download File
```http
GET /api/files/{filename}
```
*Streams binary content with automatic temporary cleanup.*

### Delete File
```http
DELETE /api/files/{filename}
```
```json
{
  "success": true,
  "filename": "test.txt",
  "message": "File 'test.txt' was successfully deleted from HDFS."
}
```

---

## 🧪 Testing

The test suite validates security constraints, route responses, and mocks HDFS subprocess execution so tests run reliably on any development machine without requiring local Hadoop:

```bash
pytest
```

---

## 🖼️ Screenshots

> *Add screenshots of the Dashboard, Files Explorer, and Drag-and-Drop Uploader here.*

---

## 🗺️ Roadmap & Future Improvements

- [ ] **Authentication & Security**: API Keys, OAuth2 / JWT authentication, and session tokens.
- [ ] **Role-Based Access Control**: Granular HDFS ACL management and per-user isolated directories.
- [ ] **Docker Deployment**: Docker Compose recipe pairing FastAPI Gateway with single-node Hadoop.
- [ ] **Multi-Directory Navigation**: Interactive tree-view folder browsing across arbitrary HDFS roots.
- [ ] **Audit Logging**: Structured query & download auditing for enterprise governance.

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).
