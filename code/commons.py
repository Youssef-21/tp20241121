import json
import logging
import sys

from fastapi.encoders import jsonable_encoder
from tinydb import Query, TinyDB


class TinyDBService:
    def __init__(self):
        self.db = TinyDB("db.json")
        self.query = Query()

    def list(self) -> list:
        return self.db.all()

    def create_or_update(self, data: dict) -> None:
        if "file" in data.keys():
            f = self.get(data["file"])
            if f is None:
                self.db.insert(data)
            else:
                self.update(data["file"], data)

    def get(self, file: str) -> dict:
        result = self.db.search(self.query.file == file)
        if len(result) > 0:
            return result[0]

    def update(self, file: str, field: dict) -> None:
        self.db.update(field, self.query.file == file)

    def delete(self, file: str) -> None:
        self.db.remove(self.query.file == file)

    def truncate(self) -> None:
        self.db.truncate()


def basemodel_to_dict(item):
    return jsonable_encoder(item)


def dict_tojson(item):
    return json.dumps(item)


def get_queue_url(sqs, queue_name):
    return sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]


def log():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
