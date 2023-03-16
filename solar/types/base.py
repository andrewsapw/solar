from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class ResponseHeader(BaseModel):
    status: int
    QTime: int
