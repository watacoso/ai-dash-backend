from fastapi import FastAPI

from app.admin.router import router as admin_router
from app.auth.router import router as auth_router
from app.charts.router import router as charts_router
from app.connections.router import router as connections_router
from app.datasets.router import router as datasets_router
from app.explore.router import router as explore_router
from app.query.router import router as query_router

app = FastAPI(title="AI-Dash API")
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(connections_router)
app.include_router(datasets_router)
app.include_router(charts_router)
app.include_router(explore_router)
app.include_router(query_router)
