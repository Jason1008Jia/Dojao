WITH rank_agg AS (
SELECT r.team_code                                          AS code,
                         r.team_name                                          AS name,
                         r.conference                                         AS conf,
                         COUNT(*)                                             AS game_cnt,
                         AVG(r.win_rate)                                      AS avg_rate,
                         SUM(CASE WHEN r.win_or_lose = 'W' THEN 1 ELSE 0 END) AS wins,
                         SUM(CASE WHEN r.win_or_lose = 'L' THEN 1 ELSE 0 END) AS losses
                  FROM mysql.ball.nba_team_rs_rank r
                  WHERE r.season = '2025'
                  GROUP BY r.team_code, r.team_name, r.conference



                  ),
     hive_player AS (SELECT p.player_name AS name
                          , p.team_code   AS code
                          , p.points      AS pts
                          , p.assists     AS ast
                          , p.rebounds    AS reb
                          , ROW_NUMBER()     OVER (PARTITION BY p.team_code ORDER BY p.points DESC) AS team_rk
                          , RANK() OVER (ORDER BY p.points DESC) AS league_rk
                          , SUM(p.points) OVER (PARTITION BY p.team_code) AS team_pts
                          , AVG(p.points) OVER (PARTITION BY p.team_code) AS team_avg_pts
                     FROM hive.ball.nba_player_stats p
                     WHERE p.season = '2025'),
     team_base AS (SELECT t.team_code  AS code,
                          t.team_name  AS name,
                          t.conference AS conf,
                          t.division   AS div,
                          t.city_name  AS city
                   FROM mysql.ball.nba_team_info t
                   WHERE t.flag = 1)
SELECT hp.name,
       hp.pts,
       hp.ast,
       hp.team_rk,
       hp.league_rk,
       hp.team_avg_pts,
       ra.game_cnt,
       ra.avg_rate,
       tb.city,
       tb.div
FROM hive_player hp
         INNER JOIN rank_agg ra ON hp.code = ra.code
         LEFT JOIN team_base tb ON hp.code = tb.code
WHERE hp.team_rk <= 5
ORDER BY hp.league_rk, hp.team_rk