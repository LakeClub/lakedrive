import os
import logging

from fastapi import FastAPI, status
from fastapi.staticfiles import StaticFiles
from starlette.responses import RedirectResponse

HTML_SOURCE = "/app/build/html"

app = FastAPI(docs_url="/api")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

if not os.path.isdir(HTML_SOURCE):
    os.makedirs(HTML_SOURCE, exist_ok=True)
app.mount("/lakedrive", StaticFiles(directory=HTML_SOURCE), name="docs")


@app.get("/")
async def root_path() -> RedirectResponse:
    return RedirectResponse(
        url="/lakedrive/index.html", status_code=status.HTTP_303_SEE_OTHER
    )


@app.get("/lakedrive")
async def lakedrive_path() -> RedirectResponse:
    return RedirectResponse(
        url="/lakedrive/index.html", status_code=status.HTTP_303_SEE_OTHER
    )
