CREATE TABLE IF NOT EXISTS animes (
    id SERIAL PRIMARY KEY,
    mal_id INTEGER UNIQUE NOT NULL, -- ID do MyAnimeList
    title VARCHAR(255) NOT NULL,
    title_english VARCHAR(255),
    type VARCHAR(50), -- TV, Movie, OVA
    episodes INTEGER,
    status VARCHAR(50), -- Finished Airing, Currently Airing
    season VARCHAR(20),
    year INTEGER,
    synopsis TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Géneros (para normalização)
CREATE TABLE IF NOT EXISTS genres (
    id SERIAL PRIMARY KEY,
    mal_id INTEGER UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL
);

-- Tabela de Associação (Muitos-para-Muitos entre Animes e Géneros)
CREATE TABLE IF NOT EXISTS anime_genres (
    anime_id INTEGER REFERENCES animes(id) ON DELETE CASCADE,
    genre_id INTEGER REFERENCES genres(id) ON DELETE CASCADE,
    PRIMARY KEY (anime_id, genre_id)
);

-- Tabela de Histórico de Métricas (Fato)
-- Aqui guardamos "snapshots" dos dados para ver a evolução temporal
CREATE TABLE IF NOT EXISTS metrics_history (
    id SERIAL PRIMARY KEY,
    anime_id INTEGER REFERENCES animes(id) ON DELETE CASCADE,
    score DECIMAL(4,2), -- Ex: 8.45
    rank INTEGER,
    popularity INTEGER,
    members INTEGER,
    favorites INTEGER,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para melhorar a performance das consultas futuras
CREATE INDEX idx_animes_mal_id ON animes(mal_id);
CREATE INDEX idx_metrics_anime_id ON metrics_history(anime_id);
CREATE INDEX idx_metrics_date ON metrics_history(collected_at);