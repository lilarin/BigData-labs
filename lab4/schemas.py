import uuid

from pydantic import BaseModel, Field


class TelecomMetric(BaseModel):
    @staticmethod
    def generate_client_id():
        return str(uuid.uuid4())

    client_id: str = Field(default_factory=generate_client_id)
    processing_time_ms: float
