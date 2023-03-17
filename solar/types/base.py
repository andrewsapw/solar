from __future__ import annotations

from pydantic import BaseModel


class ResponseHeader(BaseModel):
    status: int
    QTime: int
