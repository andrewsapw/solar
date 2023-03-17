from typing import Dict, Optional

from pydantic import BaseModel

from solar.types.base import ResponseHeader


class File(BaseModel):
    size: Optional[int] = 0
    directory: Optional[bool] = False


class ConfigFiles(BaseModel):
    responseHeader: ResponseHeader
    files: Dict[str, File]
    path: str = ""
