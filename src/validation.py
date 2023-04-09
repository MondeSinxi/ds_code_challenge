from pydantic import BaseModel
from typing import List, Optional


class FeatureProperty(BaseModel):
    index: str
    centroid_lat: float
    centroid_lon: float
    resolution: Optional[int]


class Geometry(BaseModel):
    _type: str
    coordinates: List


class Feature(BaseModel):
    _type: str
    properties: FeatureProperty
    geometry: Geometry

