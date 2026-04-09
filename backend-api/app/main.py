from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .auth import router as auth_router
from .dashboard import router as dashboard_router
from .settings import get_settings

settings = get_settings()

app = FastAPI(
    title="NeoInvest Backend API",
    version="1.0.0",
    description="Backend API for auth and MOEX market dashboard snapshots.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins if settings.cors_origins else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(auth_router)
app.include_router(dashboard_router)

