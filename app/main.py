import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import router
from app.memory import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("greyline")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("starting greyline api")
    init_db()
    yield
    logger.info("stopping greyline api")


app = FastAPI(title="greyline", version="0.1.0", lifespan=lifespan)
app.include_router(router)
