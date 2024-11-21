import sys

import boto3
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import commons, settings

logger = commons.log()

if settings.STACK_URL == "":
    logger.error("STACK_URL is not defined")
    sys.exit(1)

sqs = boto3.client("sqs", endpoint_url=settings.STACK_URL)

db_service = commons.TinyDBService()


class Asset(BaseModel):
    file: str
    bucket: str
    source_bucket: str
    channel_count: int
    frame_count: int
    sample_rate: int
    bits_per_sample: int
    duration: str


app = FastAPI(
    title="Assets",
    description="""Assets service""",
    docs_url="/",
)


@app.get("/assets")
async def list():
    return db_service.list()


@app.get("/asset/{file}")
async def get(file: str):
    asset = db_service.get(file)
    if asset is not None:
        return asset
    return JSONResponse(status_code=404, content={"message": "asset not found"})


@app.delete("/asset/{file}")
async def delete(file: str):
    asset = db_service.get(file)
    if asset is not None:
        # TODO PART IV F, delete asset on the DB with db_service.delete() and send an order with sqs.send_message() to delete file on S3
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
        logger.info(f"{asset['bucket']} - {asset['file']}")
        return JSONResponse(
            status_code=501, content={"message": "delete is not implemented"}
        )
    return JSONResponse(status_code=404, content={"message": "asset not found"})


@app.post("/asset")
async def create(data: Asset):
    asset = commons.basemodel_to_dict(data)
    db_service.create_or_update(asset)
    return asset
