from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel

from solar.types.base import ResponseHeader


class Replica(BaseModel):
    core: str
    node_name: str
    base_url: str
    state: str
    type: str
    force_set_state: str
    leader: Optional[str] = None


class Shard(BaseModel):
    range: str
    state: str
    replicas: Dict[str, Replica]
    health: str


class Router(BaseModel):
    name: str


class Collection(BaseModel):
    pullReplicas: str
    replicationFactor: str
    shards: Dict[str, Shard]
    router: Router
    maxShardsPerNode: str
    autoAddReplicas: str
    nrtReplicas: str
    tlogReplicas: str
    health: str
    znodeVersion: int
    aliases: Optional[List[str]] = None
    configName: str


class Core(BaseModel):
    core: str
    node_name: str
    base_url: str
    state: str
    type: str
    force_set_state: str
    leader: str


class Properties(BaseModel):
    urlScheme: str


class Cluster(BaseModel):
    collections: Dict[str, Collection]
    properties: Properties
    aliases: Dict
    live_nodes: List[str]


class ClusterStatus(BaseModel):
    responseHeader: ResponseHeader
    cluster: Cluster
