from fastapi import FastAPI

from app.admin.router import router as admin_router
from app.auth.router import router as auth_router
from app.connections.router import router as connections_router

app = FastAPI(title="AI-Dash API")
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(connections_router)
