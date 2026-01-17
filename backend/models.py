from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from .database import Base

class Anime(Base):
    __tablename__ = "animes"

    id = Column(Integer, primary_key=True, index=True)
    mal_id = Column(Integer, unique=True, index=True)
    title = Column(String)
    title_english = Column(String, nullable=True)
    type = Column(String)
    episodes = Column(Integer)
    status = Column(String)
    season = Column(String)
    year = Column(Integer)
    synopsis = Column(Text, nullable=True)

    # Relacionamento com o histórico (Um Anime tem Várias Métricas)
    metrics = relationship("MetricsHistory", back_populates="anime")

class MetricsHistory(Base):
    __tablename__ = "metrics_history"

    id = Column(Integer, primary_key=True, index=True)
    anime_id = Column(Integer, ForeignKey("animes.id"))
    score = Column(Float)
    rank = Column(Integer)
    popularity = Column(Integer)
    members = Column(Integer)
    favorites = Column(Integer)
    collected_at = Column(DateTime)

    anime = relationship("Anime", back_populates="metrics")