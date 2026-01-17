from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# Schema para Métricas
class MetricOut(BaseModel):
    score: Optional[float]
    rank: Optional[int]
    popularity: Optional[int]
    collected_at: datetime

    class Config:
        from_attributes = True

# Schema para Anime (Inclui a lista de métricas)
class AnimeOut(BaseModel):
    id: int
    mal_id: int
    title: str
    type: Optional[str]
    episodes: Optional[int]
    year: Optional[int]
    
    # Nested Model: Inclui o histórico dentro do JSON do anime
    metrics: List[MetricOut] = []

    class Config:
        from_attributes = True