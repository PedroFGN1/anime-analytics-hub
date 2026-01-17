from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from . import models
from . import schemas
from .database import engine, get_db

# Cria as tabelas se não existirem (segurança extra)
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Anime Analytics API")

@app.get("/")
def read_root():
    return {"message": "Bem-vindo ao Anime Analytics Hub API!"}

@app.get("/animes/", response_model=List[schemas.AnimeOut])
def read_animes(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    """
    Retorna uma lista de animes com seu histórico de métricas.
    """
    animes = db.query(models.Anime).offset(skip).limit(limit).all()
    return animes

@app.get("/animes/{anime_id}", response_model=schemas.AnimeOut)
def read_anime(anime_id: int, db: Session = Depends(get_db)):
    """
    Busca um anime específico pelo ID interno.
    """
    anime = db.query(models.Anime).filter(models.Anime.id == anime_id).first()
    if anime is None:
        raise HTTPException(status_code=404, detail="Anime não encontrado")
    return anime

### Passo 3: Rodar o Servidor
'''

1.  **Ativar o Ambiente Virtual** (se não estiver ativo):
    ```bash
    source venv/bin/activate
    ```
2.  **Instalar dependências do Backend:**
    ```bash
    pip install -r backend/requirements.txt
    ```
3.  **Iniciar a API:**
    Execute este comando dentro da pasta raiz (`anime-analytics-hub`):
    ```bash
    uvicorn backend.main:app --reload'''