import re


def derive_game_type(game_id: str) -> str:
    prefix = game_id[:3] if game_id else ""
    return {"002": "regular", "004": "playoff", "005": "play-in"}.get(prefix, "preseason")


def parse_series_wins(
    series_text: str | None,
    home_tricode: str | None,
    away_tricode: str | None,
) -> tuple[int | None, int | None]:
    """Retorna (home_wins, away_wins) a partir do seriesText da NBA.

    Formatos: "BOS leads 3-1", "BOS wins 4-2", "Series tied 2-2".
    Retorna (None, None) se vazio ou sem placar reconhecivel.
    """
    if not series_text:
        return None, None

    score_match = re.search(r"(\d+)-(\d+)", series_text)
    if not score_match:
        return None, None

    score_a, score_b = int(score_match.group(1)), int(score_match.group(2))

    leader_match = re.search(r"^(\w+)\s+(?:leads|wins)", series_text)
    if not leader_match:
        return score_a, score_a  # "Series tied X-X"

    leader = leader_match.group(1)
    if leader == home_tricode:
        return score_a, score_b
    if leader == away_tricode:
        return score_b, score_a

    return None, None
