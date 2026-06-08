# NBA Data Pipeline — Airflow · PySpark · Delta Lake · PostgreSQL

Pipeline de dados end-to-end que coleta estatísticas da NBA em tempo real, processa em camadas Bronze → Silver → Gold e entrega resultados automaticamente via bot no Telegram. Os dados estruturados em Star Schema no PostgreSQL estão prontos para análise em qualquer ferramenta de BI.

---

## Arquitetura

```mermaid
graph TD
    subgraph EXT["Extração · Bronze"]
        A1["NBA CDN\nschedule + broadcaster BR"]
        A3["NBA API\nboxscores"]
    end

    subgraph SIL["Transformação · Silver / Delta Lake"]
        B1["schedule\ncalendário + transmissão BR"]
        B2["boxscores\nresultado por jogo"]
        B3["player_stats\nstats individuais"]
    end

    subgraph GLD["Carga · Gold / PostgreSQL"]
        C1[("dim_nba_schedule")]
        C2[("fact_nba_boxscores")]
        C3[("dim_players")]
        C4[("fact_player_game_stats")]
    end

    subgraph DEL["Entrega"]
        D1["Bot Telegram\nresultados + enquetes"]
        D2["BI Dashboard\nMetabase / Power BI"]
    end

    A1 --> B1
    A3 --> B2
    A3 --> B3
    B1 --> C1
    B2 --> C2
    B3 --> C3
    B3 --> C4
    C1 & C2 --> D1
    C1 & C2 & C3 & C4 --> D2
```

---

## Stack

| Tecnologia | Papel |
|---|---|
| **Apache Airflow** | Orquestração dos DAGs diário e backfill |
| **PySpark + Delta Lake** | Transformação Bronze → Silver com ACID e schema enforcement |
| **delta-rs (deltalake)** | Leitura de Delta tables em pandas sem overhead de JVM |
| **PostgreSQL** | Camada Gold em Star Schema para queries analíticas |
| **SQLAlchemy** | Upsert idempotente via ON CONFLICT DO UPDATE |
| **Python / Requests** | Extração da API e CDN da NBA |
| **python-telegram-bot** | Envio de resultados e enquetes ao grupo do Telegram |
| **Docker Compose** | Ambiente reproduzível com Airflow, Postgres e Workers |
| **uv** | Gerenciamento de dependências Python |

---

## Camadas de dados

### Bronze
JSON bruto exatamente como retornado pela fonte. Imutável. Serve como fonte da verdade para reprocessamento.

### Silver
Dados transformados e validados, armazenados em **Delta Lake**. Garante:
- Schema enforced com tipos corretos e campos selecionados
- Reprocessamento idempotente via `mode="overwrite"` por data
- Suporte a time travel e rollback pelo Delta log

### Gold (PostgreSQL)
Star Schema com quatro tabelas:

| Tabela | Descrição |
|---|---|
| `dim_nba_schedule` | Calendário de jogos com times, horário e transmissão no Brasil |
| `dim_players` | Dimensão de jogadores com nome e posição |
| `fact_nba_boxscores` | Resultado por jogo com placar e time vencedor |
| `fact_player_game_stats` | Stats individuais por jogo: pts, reb, ast, stl, blk, +/- e mais 15 métricas |

Temporada 2025-26 completa: **1.402 jogos**, **683 jogadores**, **30.740 registros** de player stats.

```mermaid
erDiagram
    dim_nba_schedule {
        string game_id PK
        timestamp game_datetime_utc
        string home_team_name
        string away_team_name
        string brazil_broadcaster
        string game_type
        int home_series_wins
        int away_series_wins
    }

    fact_nba_boxscores {
        string game_id PK
        int home_score
        int away_score
        string winner_team
        int winner_team_id
    }

    dim_players {
        int player_id PK
        string player_name
        string position
    }

    fact_player_game_stats {
        string stat_id PK
        string game_id FK
        int player_id FK
        string team_name
        string side
        boolean starter
        string minutes
        int pts
        int reb
        int ast
        int stl
        int blk
        int tov
        int plus_minus
        float fg_pct
        float tp_pct
        float ft_pct
    }

    dim_nba_schedule ||--o{ fact_nba_boxscores : "game_id"
    dim_nba_schedule ||--o{ fact_player_game_stats : "game_id"
    dim_players ||--o{ fact_player_game_stats : "player_id"
```

---

## DAGs

### `nba_etl_daily_pipeline`

Roda diariamente às 08h00 UTC (05h00 BRT). Janela escolhida para garantir que jogos
com tip-off à 01h00 BRT (~03h30 de duração) já tenham boxscores finalizados na CDN da NBA.

```mermaid
graph LR
    ES[extract_schedule] --> SS[silver_schedule]
    EB[extract_boxscores] --> SB[silver_boxscores]
    EB --> SP[silver_player_stats]
    SS & SB & SP --> GL[load_gold]
    GL --> BR[bot_resultados]
    BR --> BE[bot_enquetes]
```

### `nba_backfill_current_season`

Trigger manual. Processa toda a temporada atual do zero.

```mermaid
graph LR
    E[extract_all_boxscores] --> S[transform_all_silver]
    S --> G[load_all_gold]
```

### `nba_polls_stopper`
Roda a cada 10 minutos. Fecha enquetes do Telegram de jogos que já terminaram.

---

## Bot Telegram

O bot publica automaticamente no grupo configurado:
- **Resultados**: placar final, time vencedor e destaques do jogo
- **Enquetes**: votação sobre o vencedor antes do jogo começar

As credenciais (`BOT_TOKEN`, `GROUP_ID`) ficam no `.env` local, nunca commitadas.

---

## Possibilidades analíticas com os dados

O PostgreSQL com Star Schema conecta nativamente a ferramentas de BI como Metabase, Power BI e Superset. Com os dados atuais já é possível construir:

- Ranking de artilheiros e líderes em rebotes e assistências da temporada
- Evolução de performance de um jogador ao longo dos jogos
- Comparativo de eficiência entre times
- Distribuição de minutos e stats por posição
- Análise de +/- por jogador e contexto de jogo

Isso está planejado como **Fase 8**: adicionar Metabase ao `docker-compose.yml` apontando para o mesmo PostgreSQL existente, sem nenhuma extração ou transformação nova.

---

## Como rodar localmente

### Pré-requisitos
- Docker e Docker Compose
- Python 3.12+ com [uv](https://github.com/astral-sh/uv)

### Setup

```bash
# 1. Clone o repositório
git clone https://github.com/LucasEmanuell/nba_pipeline.git
cd nba_pipeline

# 2. Configure as variáveis de ambiente
cp .env.example .env
# Edite .env com suas credenciais do Telegram

# 3. Suba o ambiente
docker compose up -d

# 4. Aguarde ~30s e acesse o Airflow em http://localhost:8080
# Credenciais padrão do Airflow local: airflow / airflow

# 5. Corrija permissões de escrita nos volumes
# Necessário pelo UID diferente entre host (1000) e container Airflow (50000)
docker exec --user root $(docker ps -qf "name=airflow-worker") chmod -R 777 /opt/airflow/data/

# 6. Ative e dispare o backfill manualmente na UI do Airflow
# DAG: nba_backfill_current_season → Trigger DAG
```

### Variáveis de ambiente necessárias

```env
BOT_TOKEN=          # Token do bot no BotFather
GROUP_ID=           # ID do grupo Telegram
DB_URL_INTERNAL=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
DB_URL_EXTERNAL=postgresql+psycopg2://airflow:airflow@localhost:5433/airflow
AIRFLOW_UID=1000
```

---

## Estrutura do projeto

```
nba_pipeline/
├── dags/
│   ├── nba_daily_dag.py          # Pipeline diário completo
│   ├── nba_backfill_dag.py       # Backfill da temporada atual
│   └── nba_polls_stopper_dag.py  # Fechamento de enquetes
├── src/
│   ├── extract/                  # Extração Bronze (CDN + API)
│   ├── transform/                # Silver (PySpark/Delta) e Gold (PostgreSQL)
│   │   └── derivations.py        # Funções puras de derivação (game_type, series_wins)
│   └── bot/                      # Bots de resultados e enquetes
├── tests/
│   ├── conftest.py               # Fixture de conexão com o banco
│   ├── test_data_quality.py      # 16 testes de contrato contra o Gold
│   └── test_derivations.py       # 12 testes unitários de lógica de negócio
├── docker-compose.yml
├── Dockerfile
└── pyproject.toml
```

---

## Testes

```bash
# Testes unitários (sem banco, sem Spark)
uv run pytest tests/test_derivations.py -v

# Testes de qualidade de dados contra o Gold (requer Docker rodando)
uv run pytest tests/test_data_quality.py -v

# Suite completa
uv run pytest tests/ -v
```

Os testes de qualidade usam `DB_URL_EXTERNAL` do `.env` — configure antes de rodar.

---

## Roadmap

- [x] Fase 1 — Extração do calendário (Bronze)
- [x] Fase 2 — Silver com Delta Lake
- [x] Fase 3 — Gold com PostgreSQL + bots Telegram
- [x] Fase 4 — Data quality checks na Silver
- [x] Fase 5 — Backfill da temporada 2025-26
- [x] Fase 6 — Stats individuais de jogadores (Star Schema)
- [x] Fase 7 — Testes automatizados (unitários + contrato Gold)
- [ ] Fase 8 — Dashboard BI (Metabase + PostgreSQL existente)
- [ ] Fase 9 — Stats avançados (PER, TS%, usage rate)
