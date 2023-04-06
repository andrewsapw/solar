from __future__ import annotations

from typing import Any, Dict, List, Union

from pydantic import BaseModel

from solar.types.base import ResponseHeader


class Content(BaseModel):
    index: List[Union[str, List[Dict]]]


class FieldNames(BaseModel):
    content: Content


class Analysis(BaseModel):
    field_types: Dict[str, Any]
    field_names: FieldNames


class AnalysisModel(BaseModel):
    responseHeader: ResponseHeader
    analysis: Analysis
