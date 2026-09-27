from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.api.endpoints import router as api_router
from app.core.config import get_settings
from app.services.hdfs_service import hdfs_service

settings = get_settings()

app = FastAPI(
    title=settings.APP_NAME,
    description="REST Gateway for Hadoop HDFS operations (upload, download, list, delete)",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files & Template setup
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

static_dir = BASE_DIR / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Include API Router
app.include_router(api_router)


@app.get("/", summary="Dashboard UI")
async def render_dashboard(request: Request):
    """Render the web UI for HDFS file management."""
    hdfs_health = await hdfs_service.check_health()
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "app_name": settings.APP_NAME,
            "hdfs_status": hdfs_health,
            "current_path": settings.HDFS_DEFAULT_DIR,
            "namenode_url": settings.HDFS_NAMENODE_URL,
            "hdfs_user": settings.HDFS_USER,
            "port": settings.PORT,
        },
    )


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
    )
