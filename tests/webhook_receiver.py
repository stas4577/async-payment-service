import logging

from fastapi import FastAPI, Request, Response

app = FastAPI()
logger = logging.getLogger("uvicorn.error")


@app.post("/webhook")
async def webhook(request: Request):
    logger.info("Webhook: %s", await request.json())
    return Response(status_code=204)


@app.post("/unavailable")
async def unavailable():
    return Response(status_code=503)
