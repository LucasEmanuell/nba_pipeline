from src.transform.derivations import derive_game_type, parse_series_wins


class TestDeriveGameType:
    def test_regular(self):
        assert derive_game_type("0022500001") == "regular"

    def test_playoff(self):
        assert derive_game_type("0042500001") == "playoff"

    def test_play_in(self):
        assert derive_game_type("0052500001") == "play-in"

    def test_preseason(self):
        assert derive_game_type("0012500001") == "preseason"

    def test_all_star_cai_em_preseason(self):
        assert derive_game_type("0032500001") == "preseason"


class TestParseSeriesWins:
    def test_home_liderando(self):
        assert parse_series_wins("BOS leads 3-1", "BOS", "NYK") == (3, 1)

    def test_away_liderando(self):
        assert parse_series_wins("NYK leads 3-1", "BOS", "NYK") == (1, 3)

    def test_empatado(self):
        assert parse_series_wins("Series tied 2-2", "BOS", "NYK") == (2, 2)

    def test_serie_encerrada(self):
        assert parse_series_wins("BOS wins 4-2", "BOS", "NYK") == (4, 2)

    def test_texto_vazio(self):
        assert parse_series_wins("", "BOS", "NYK") == (None, None)

    def test_texto_none(self):
        assert parse_series_wins(None, "BOS", "NYK") == (None, None)

    def test_game_1_sem_placar(self):
        assert parse_series_wins("BOS vs NYK", "BOS", "NYK") == (None, None)
