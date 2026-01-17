import os
import time
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- Configuração de Logs ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# --- Correção de Caminho do .env ---
# Pega o diretório onde ESTE arquivo (etl_pipeline.py) está
current_dir = os.path.dirname(os.path.abspath(__file__))
# Procura o .env um nível acima (na raiz do projeto)
dotenv_path = os.path.join(current_dir, '..', '.env')

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    logger.info(f"Arquivo .env carregado de: {dotenv_path}")
else:
    logger.warning("Arquivo .env NÃO encontrado! Usando padrões ou variáveis de sistema.")

# --- Configuração do Banco ---
# Se rodar no WSL/Windows acessando Docker, localhost geralmente funciona.
# Se der erro, tente substituir 'localhost' por '127.0.0.1'
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASSWORD", "admin")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "anime_analytics")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def extract_top_animes(limit=50):
    logger.info("--- PASSO 1: EXTRAÇÃO ---")
    base_url = "https://api.jikan.moe/v4/top/anime"
    all_data = []
    page = 1
    
    while len(all_data) < limit:
        url = f"{base_url}?page={page}"
        logger.info(f"Baixando: {url}")
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                items = data.get('data', [])
                if not items:
                    logger.warning("API retornou lista vazia.")
                    break
                all_data.extend(items)
                logger.info(f"Página {page}: +{len(items)} animes. Total: {len(all_data)}")
            else:
                logger.error(f"Erro API: Status {response.status_code}")
                break
                
        except Exception as e:
            logger.error(f"Exceção na requisição: {e}")
            break
            
        page += 1
        time.sleep(1) # Respeito à API
        
    return all_data[:limit]

def load_data_debug(animes_df, metrics_df):
    logger.info("--- PASSO 3: CARGA (LOAD) ---")
    
    if animes_df.empty:
        logger.error("ERRO CRÍTICO: DataFrame de Animes está vazio. Nada a salvar.")
        return

    try:
        engine = create_engine(DATABASE_URL)
        logger.info(f"Conectando ao banco: {DATABASE_URL.split('@')[1]}") # Log seguro (sem senha)
        
        with engine.connect() as conn:
            # Iniciar Transação
            trans = conn.begin()
            
            try:
                # 1. Inserir Animes
                logger.info(f"Tentando inserir {len(animes_df)} animes...")
                
                # Converter para dicionários
                animes_dict = animes_df.to_dict(orient='records')
                
                stmt = text("""
                    INSERT INTO animes (mal_id, title, title_english, type, episodes, status, season, year, synopsis)
                    VALUES (:mal_id, :title, :title_english, :type, :episodes, :status, :season, :year, :synopsis)
                    ON CONFLICT (mal_id) 
                    DO UPDATE SET 
                        title = EXCLUDED.title,
                        updated_at = CURRENT_TIMESTAMP;
                """)
                
                conn.execute(stmt, animes_dict)
                logger.info("Animes inseridos/atualizados com sucesso.")

                # 2. Buscar IDs
                logger.info("Buscando IDs gerados...")
                mapping_result = conn.execute(text("SELECT mal_id, id FROM animes"))
                id_map = {row.mal_id: row.id for row in mapping_result}
                logger.info(f"IDs mapeados encontrados: {len(id_map)}")

                # 3. Inserir Métricas
                metrics_data = []
                for _, row in metrics_df.iterrows():
                    internal_id = id_map.get(row['mal_id'])
                    if internal_id:
                        entry = row.to_dict()
                        entry['anime_id'] = internal_id
                        del entry['mal_id']
                        metrics_data.append(entry)
                
                if metrics_data:
                    logger.info(f"Inserindo {len(metrics_data)} registros de métricas...")
                    conn.execute(
                        text("""
                            INSERT INTO metrics_history (anime_id, score, rank, popularity, members, favorites, collected_at)
                            VALUES (:anime_id, :score, :rank, :popularity, :members, :favorites, :collected_at)
                        """),
                        metrics_data
                    )
                
                # COMMIT FINAL
                trans.commit()
                logger.info("✅ COMMIT REALIZADO COM SUCESSO! Verifique o banco.")
                
            except Exception as e:
                trans.rollback()
                logger.error(f"❌ Erro durante a transação SQL: {e}")
                raise e
                
    except Exception as e:
        logger.error(f"❌ Erro de Conexão com o Banco: {e}")

# Reutilizando a transformação do script anterior, pois estava correta
def transform_data(raw_data):
    logger.info("--- PASSO 2: TRANSFORMAÇÃO ---")
    if not raw_data: return pd.DataFrame(), pd.DataFrame()
    
    df = pd.DataFrame(raw_data)
    
    # Tratamento básico para evitar erros de colunas inexistentes
    cols_animes = ['mal_id', 'title', 'title_english', 'type', 'episodes', 'status', 'season', 'year', 'synopsis']
    for col in cols_animes:
        if col not in df.columns: df[col] = None # Garante que a coluna existe
        
    animes_df = df[cols_animes].copy()
    animes_df['year'] = animes_df['year'].fillna(0).astype(int)
    
    metrics_df = df[['mal_id', 'score', 'rank', 'popularity', 'members', 'favorites']].copy()
    metrics_df['collected_at'] = pd.Timestamp.now()
    
    return animes_df, metrics_df

if __name__ == "__main__":
    raw = extract_top_animes(10) # Testando com apenas 10
    df_a, df_m = transform_data(raw)
    load_data_debug(df_a, df_m)

### 2. Executar o Teste

'''
python etl/etl_pipeline-v2.py
'''