import os
import time
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv

# --- Configuração Inicial ---
# Carrega variáveis do arquivo .env (na raiz do projeto)
load_dotenv(dotenv_path="../.env")

# Configuração de Logs (Para parecer profissional, não usamos print)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuração do Banco de Dados
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASSWORD", "admin")
DB_HOST = "localhost" # Como rodamos o script fora do Docker (no WSL), acessamos localhost
DB_PORT = "5432"
DB_NAME = os.getenv("DB_NAME", "anime_analytics")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Funções ETL ---

def extract_top_animes(limit=50):
    """
    EXTRACT: Busca os top animes da API Jikan.
    Lida com paginação e rate limiting.
    """
    logger.info("Iniciando extração de dados da Jikan API...")
    base_url = "https://api.jikan.moe/v4/top/anime"
    all_data = []
    page = 1
    
    while len(all_data) < limit:
        try:
            response = requests.get(f"{base_url}?page={page}")
            
            # Tratamento de Rate Limit (HTTP 429)
            if response.status_code == 429:
                logger.warning("Rate limit atingido. Aguardando 5 segundos...")
                time.sleep(5)
                continue
            
            response.raise_for_status()
            data = response.json()
            
            items = data.get('data', [])
            if not items:
                break
                
            all_data.extend(items)
            logger.info(f"Página {page} processada. Total coletado: {len(all_data)}")
            
            page += 1
            time.sleep(1) # Respeitar a API (politeness)
            
        except Exception as e:
            logger.error(f"Erro na extração: {e}")
            break
            
    return all_data[:limit]

def transform_data(raw_data):
    """
    TRANSFORM: Limpa e normaliza os dados usando Pandas.
    Separa o que é dado estático (tabela animes) do que é histórico (tabela metrics).
    """
    logger.info("Transformando dados brutos...")
    
    if not raw_data:
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(raw_data)

    # 1. Preparar dados para a tabela 'animes' (Dimensão)
    # Selecionamos colunas e renomeamos para bater com o banco
    animes_df = df[[
        'mal_id', 'title', 'title_english', 'type', 
        'episodes', 'status', 'season', 'year', 'synopsis'
    ]].copy()
    
    # Tratamento de nulos
    animes_df['year'] = animes_df['year'].fillna(0).astype(int)
    
    # 2. Preparar dados para a tabela 'metrics_history' (Fato)
    metrics_df = df[[
        'mal_id', 'score', 'rank', 'popularity', 'members', 'favorites'
    ]].copy()
    
    # Adicionar timestamp da coleta
    metrics_df['collected_at'] = pd.Timestamp.now()
    
    return animes_df, metrics_df

def load_data(animes_df, metrics_df):
    """
    LOAD: Insere os dados no PostgreSQL.
    Usa lógica de UPSERT (Update se existir, Insert se novo).
    """
    if animes_df.empty:
        logger.warning("Nenhum dado para carregar.")
        return

    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.begin() as conn:
            # 1. Carga da tabela ANIMES (Upsert)
            logger.info("Carregando tabela 'animes'...")
            
            # Iteramos converte o DF para dicionário para usar SQL
            for _, row in animes_df.iterrows():
                stmt = text("""
                    INSERT INTO animes (mal_id, title, title_english, type, episodes, status, season, year, synopsis)
                    VALUES (:mal_id, :title, :title_english, :type, :episodes, :status, :season, :year, :synopsis)
                    ON CONFLICT (mal_id) 
                    DO UPDATE SET 
                        title = EXCLUDED.title,
                        status = EXCLUDED.status,
                        episodes = EXCLUDED.episodes,
                        updated_at = CURRENT_TIMESTAMP;
                """)
                conn.execute(stmt, row.to_dict())
            
            # 2. Precisamos buscar os IDs internos (PK) dos animes para inserir nas métricas
            # (Porque a métrica usa o 'id' do banco, não o 'mal_id')
            logger.info("Mapeando IDs internos...")
            
            # Criar mapa mal_id -> id
            mapping_result = conn.execute(text("SELECT mal_id, id FROM animes"))
            id_map = {row.mal_id: row.id for row in mapping_result}
            
            # 3. Carga da tabela METRICS_HISTORY (Append Only)
            logger.info("Carregando tabela 'metrics_history'...")
            
            metrics_data = []
            for _, row in metrics_df.iterrows():
                internal_id = id_map.get(row['mal_id'])
                if internal_id:
                    metrics_entry = row.to_dict()
                    metrics_entry['anime_id'] = internal_id
                    del metrics_entry['mal_id'] # Remove mal_id pois usamos FK anime_id
                    metrics_data.append(metrics_entry)
            
            if metrics_data:
                conn.execute(
                    text("""
                        INSERT INTO metrics_history (anime_id, score, rank, popularity, members, favorites, collected_at)
                        VALUES (:anime_id, :score, :rank, :popularity, :members, :favorites, :collected_at)
                    """),
                    metrics_data
                )
                
        logger.info("Carga concluída com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro ao conectar ou salvar no banco: {e}")

# --- Execução Principal ---
if __name__ == "__main__":
    start_time = time.time()
    
    # 1. Extrair
    raw_data = extract_top_animes(limit=50)
    
    # 2. Transformar
    df_animes, df_metrics = transform_data(raw_data)
    
    # 3. Carregar
    load_data(df_animes, df_metrics)
    
    logger.info(f"Pipeline finalizado em {time.time() - start_time:.2f} segundos.")

### Instruções de Implementação
'''
Aqui está o passo-a-passo para rodares este pipeline no teu WSL:

1.  **Criar o Ambiente Virtual (Boas práticas):**
    No terminal WSL, dentro da pasta `anime-analytics-hub`:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2.  **Instalar Dependências:**
    Certifica-te que criaste a pasta `etl` e o ficheiro `requirements.txt` dentro dela.
    ```bash
    pip install -r etl/requirements.txt
    ```
    *(Nota: Se tiveres erro no `psycopg2`, podes precisar de instalar bibliotecas do sistema: `sudo apt-get install libpq-dev` no Ubuntu/WSL).*

3.  **Rodar o ETL:**
    ```bash
    python etl/etl_pipeline.py
'''