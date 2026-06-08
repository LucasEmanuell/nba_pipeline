"""
Testes de qualidade de dados contra o PostgreSQL Gold.

Cada teste representa um invariante do negócio: se falhar, algo errado
chegou até a camada de consumo. Rodar antes de qualquer entrega garante
que o analista não vai trabalhar com dado corrompido.
"""
from sqlalchemy import text


# ── Integridade referencial ───────────────────────────────────────────────────

def test_boxscores_game_ids_existem_no_schedule(conn):
    """Todo game_id de boxscores deve existir no calendário."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_nba_boxscores b
        LEFT JOIN dim_nba_schedule s ON b.game_id = s.game_id
        WHERE s.game_id IS NULL
    """)).scalar()
    assert result == 0, f"{result} boxscores com game_id sem correspondência no schedule"


def test_player_stats_game_ids_existem_no_schedule(conn):
    """Todo game_id de player stats deve existir no calendário."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_player_game_stats p
        LEFT JOIN dim_nba_schedule s ON p.game_id = s.game_id
        WHERE s.game_id IS NULL
    """)).scalar()
    assert result == 0, f"{result} player stats com game_id sem correspondência no schedule"


def test_player_stats_player_ids_existem_na_dim(conn):
    """Todo player_id de player stats deve existir em dim_players."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_player_game_stats p
        LEFT JOIN dim_players d ON p.player_id = d.player_id
        WHERE d.player_id IS NULL
    """)).scalar()
    assert result == 0, f"{result} player stats com player_id sem correspondência em dim_players"


# ── Completeness ──────────────────────────────────────────────────────────────

def test_jogos_finalizados_tem_boxscore(conn):
    """Todo jogo com game_status=3 (finalizado) deve ter boxscore."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM dim_nba_schedule s
        LEFT JOIN fact_nba_boxscores b ON s.game_id = b.game_id
        WHERE s.game_status = 3
          AND b.game_id IS NULL
    """)).scalar()
    assert result == 0, f"{result} jogos finalizados sem boxscore"


def test_jogos_finalizados_tem_player_stats(conn):
    """Todo jogo finalizado deve ter ao menos um registro de player stats."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_nba_boxscores b
        LEFT JOIN fact_player_game_stats p ON b.game_id = p.game_id
        WHERE b.is_final = TRUE
          AND p.game_id IS NULL
    """)).scalar()
    assert result == 0, f"{result} jogos finalizados sem player stats"


def test_todo_jogo_tem_dois_lados(conn):
    """Todo jogo finalizado deve ter jogadores de exatamente dois lados: home e away."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT game_id, COUNT(DISTINCT side) AS lados
            FROM fact_player_game_stats
            GROUP BY game_id
            HAVING COUNT(DISTINCT side) != 2
        ) t
    """)).scalar()
    assert result == 0, f"{result} jogos com número de lados diferente de 2"


# ── Validade de valores ───────────────────────────────────────────────────────

def test_scores_nao_negativos(conn):
    """Placar nunca pode ser negativo."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_nba_boxscores
        WHERE home_score < 0 OR away_score < 0
    """)).scalar()
    assert result == 0, f"{result} jogos com placar negativo"


def test_scores_minimo_realista(conn):
    """Placar abaixo de 60 em temporada regular ou playoffs indica dado corrompido.
    Exclui All-Star (003) pois têm formato diferente com quartos mais curtos.
    """
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_nba_boxscores b
        JOIN dim_nba_schedule s ON b.game_id = s.game_id
        WHERE b.is_final = TRUE
          AND s.game_type IN ('regular', 'playoff', 'play-in')
          AND (b.home_score < 60 OR b.away_score < 60)
    """)).scalar()
    assert result == 0, f"{result} jogos (excluindo All-Star) com placar abaixo de 60"


def test_winner_valores_validos(conn):
    """Campo winner só pode ser 'home', 'away' ou NULL."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_nba_boxscores
        WHERE winner NOT IN ('home', 'away')
          AND winner IS NOT NULL
    """)).scalar()
    assert result == 0, f"{result} boxscores com valor inválido em winner"


def test_game_type_valores_validos(conn):
    """game_type só pode ter os quatro valores definidos pela NBA."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM dim_nba_schedule
        WHERE game_type NOT IN ('regular', 'playoff', 'play-in', 'preseason')
    """)).scalar()
    assert result == 0, f"{result} jogos com game_type fora do domínio"


def test_side_valores_validos(conn):
    """side em player stats só pode ser 'home' ou 'away'."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_player_game_stats
        WHERE side NOT IN ('home', 'away')
    """)).scalar()
    assert result == 0, f"{result} registros com side inválido"


def test_percentuais_entre_zero_e_um(conn):
    """fg_pct, tp_pct e ft_pct devem estar entre 0 e 1."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_player_game_stats
        WHERE (fg_pct < 0 OR fg_pct > 1)
           OR (tp_pct < 0 OR tp_pct > 1)
           OR (ft_pct < 0 OR ft_pct > 1)
    """)).scalar()
    assert result == 0, f"{result} registros com percentual fora do intervalo [0, 1]"


def test_pontos_nao_negativos(conn):
    """Pontos nunca podem ser negativos."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_player_game_stats
        WHERE pts < 0
    """)).scalar()
    assert result == 0, f"{result} registros com pontos negativos"


# ── Consistência ──────────────────────────────────────────────────────────────

def test_winner_consistente_com_placar(conn):
    """Se home_score > away_score, winner deve ser 'home', e vice-versa."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_nba_boxscores
        WHERE is_final = TRUE
          AND (
            (home_score > away_score AND winner != 'home')
            OR
            (away_score > home_score AND winner != 'away')
          )
    """)).scalar()
    assert result == 0, f"{result} jogos com winner inconsistente com o placar"


def test_pk_unica_em_player_stats(conn):
    """A chave game_id + player_id não pode se repetir em player stats."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT game_id, player_id, COUNT(*) AS n
            FROM fact_player_game_stats
            GROUP BY game_id, player_id
            HAVING COUNT(*) > 1
        ) t
    """)).scalar()
    assert result == 0, f"{result} combinações game_id+player_id duplicadas"


def test_fgm_nao_supera_fga(conn):
    """Cestas convertidas não podem superar tentativas."""
    result = conn.execute(text("""
        SELECT COUNT(*) FROM fact_player_game_stats
        WHERE fgm > fga
           OR tpm > tpa
           OR ftm > fta
    """)).scalar()
    assert result == 0, f"{result} registros com made > attempted"
