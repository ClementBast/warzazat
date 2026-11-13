from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

GAME = StructType([
    StructField("season_id", StringType()),
    StructField("team_id_home", StringType()),
    StructField("team_abbreviation_home", StringType()),
    StructField("team_name_home", StringType()),
    StructField("game_id", StringType()),
    StructField("game_date", StringType()),
    StructField("matchup_home", StringType()),
    StructField("wl_home", StringType()),
    StructField("min", DoubleType()),
    StructField("fgm_home", DoubleType()),
    StructField("fga_home", DoubleType()),
    StructField("fg_pct_home", DoubleType()),
    StructField("fg3m_home", DoubleType()),
    StructField("fg3a_home", DoubleType()),
    StructField("fg3_pct_home", DoubleType()),
    StructField("ftm_home", DoubleType()),
    StructField("fta_home", DoubleType()),
    StructField("ft_pct_home", DoubleType()),
    StructField("oreb_home", DoubleType()),
    StructField("dreb_home", DoubleType()),
    StructField("reb_home", DoubleType()),
    StructField("ast_home", DoubleType()),
    StructField("stl_home", DoubleType()),
    StructField("blk_home", DoubleType()),
    StructField("tov_home", DoubleType()),
    StructField("pf_home", DoubleType()),
    StructField("pts_home", DoubleType()),
    StructField("plus_minus_home", DoubleType()),
    StructField("video_available_home", StringType()),
    StructField("team_id_away", StringType()),
    StructField("team_abbreviation_away", StringType()),
    StructField("team_name_away", StringType()),
    StructField("matchup_away", StringType()),
    StructField("wl_away", StringType()),
    StructField("fgm_away", DoubleType()),
    StructField("fga_away", DoubleType()),
    StructField("fg_pct_away", DoubleType()),
    StructField("fg3m_away", DoubleType()),
    StructField("fg3a_away", DoubleType()),
    StructField("fg3_pct_away", DoubleType()),
    StructField("ftm_away", DoubleType()),
    StructField("fta_away", DoubleType()),
    StructField("ft_pct_away", DoubleType()),
    StructField("oreb_away", DoubleType()),
    StructField("dreb_away", DoubleType()),
    StructField("reb_away", DoubleType()),
    StructField("ast_away", DoubleType()),
    StructField("stl_away", DoubleType()),
    StructField("blk_away", DoubleType()),
    StructField("tov_away", DoubleType()),
    StructField("pf_away", DoubleType()),
    StructField("pts_away", DoubleType()),
    StructField("plus_minus_away", DoubleType()),
    StructField("video_available_away", StringType()),
    StructField("season_type", StringType()),
])

LINE_SCORE = StructType(
    [StructField("game_date_est", StringType()),
     StructField("game_sequence", StringType()),
     StructField("game_id", StringType())] +
    [StructField(c, StringType()) for c in
     ["team_id_home", "team_abbreviation_home", "team_city_name_home",
      "team_nickname_home", "team_wins_losses_home"]] +
    [StructField(c, DoubleType()) for c in
     ["pts_qtr1_home", "pts_qtr2_home", "pts_qtr3_home", "pts_qtr4_home"] +
     [f"pts_ot{i}_home" for i in range(1, 11)] + ["pts_home"]] +
    [StructField(c, StringType()) for c in
     ["team_id_away", "team_abbreviation_away", "team_city_name_away",
      "team_nickname_away", "team_wins_losses_away"]] +
    [StructField(c, DoubleType()) for c in
     ["pts_qtr1_away", "pts_qtr2_away", "pts_qtr3_away", "pts_qtr4_away"] +
     [f"pts_ot{i}_away" for i in range(1, 11)] + ["pts_away"]])

GAME_INFO = StructType([
    StructField("game_id", StringType()),
    StructField("game_date", StringType()),
    StructField("attendance", DoubleType()),
    StructField("game_time", StringType()),
])

GAME_SUMMARY = StructType([
    StructField("game_date_est", StringType()),
    StructField("game_sequence", StringType()),
    StructField("game_id", StringType()),
    StructField("game_status_id", IntegerType()),
    StructField("game_status_text", StringType()),
    StructField("gamecode", StringType()),
    StructField("home_team_id", StringType()),
    StructField("visitor_team_id", StringType()),
    StructField("season", IntegerType()),
    StructField("live_period", IntegerType()),
    StructField("live_pc_time", StringType()),
    StructField("natl_tv_broadcaster_abbreviation", StringType()),
    StructField("live_period_time_bcast", StringType()),
    StructField("wh_status", IntegerType()),
])

OTHER_STATS = StructType(
    [StructField(c, StringType()) for c in
     ["game_id", "league_id", "team_id_home", "team_abbreviation_home", "team_city_home"]] +
    [StructField(c, DoubleType()) for c in
     ["pts_paint_home", "pts_2nd_chance_home", "pts_fb_home", "largest_lead_home",
      "lead_changes", "times_tied", "team_turnovers_home", "total_turnovers_home",
      "team_rebounds_home", "pts_off_to_home"]] +
    [StructField(c, StringType()) for c in
     ["team_id_away", "team_abbreviation_away", "team_city_away"]] +
    [StructField(c, DoubleType()) for c in
     ["pts_paint_away", "pts_2nd_chance_away", "pts_fb_away", "largest_lead_away",
      "team_turnovers_away", "total_turnovers_away", "team_rebounds_away", "pts_off_to_away"]])

PLAY_BY_PLAY = StructType(
    [StructField("game_id", StringType()),
     StructField("eventnum", IntegerType()),
     StructField("eventmsgtype", IntegerType()),
     StructField("eventmsgactiontype", IntegerType()),
     StructField("period", IntegerType()),
     StructField("wctimestring", StringType()),
     StructField("pctimestring", StringType()),
     StructField("homedescription", StringType()),
     StructField("neutraldescription", StringType()),
     StructField("visitordescription", StringType()),
     StructField("score", StringType()),
     StructField("scoremargin", DoubleType())] +
    [StructField(c, t) for i in (1, 2, 3) for c, t in
     [(f"person{i}type", IntegerType()), (f"player{i}_id", StringType()),
      (f"player{i}_name", StringType()), (f"player{i}_team_id", StringType()),
      (f"player{i}_team_city", StringType()), (f"player{i}_team_nickname", StringType()),
      (f"player{i}_team_abbreviation", StringType())]] +
    [StructField("video_available_flag", IntegerType())])

SCHEMAS = {"game": GAME, "line_score": LINE_SCORE, "game_info": GAME_INFO,
           "game_summary": GAME_SUMMARY, "other_stats": OTHER_STATS,
           "play_by_play": PLAY_BY_PLAY}
