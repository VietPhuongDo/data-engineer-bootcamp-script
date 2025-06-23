select * from players;

create table player_scd(
    player_name text,
    scoring_class scoring_class,
    is_active boolean,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,
    primary key (player_name,start_season)
);


insert into player_scd
with with_previous as(
    select player_name,
           scoring_class,
           current_season,
           is_active,
           lag(scoring_class,1) over (partition by player_name order by current_season) as previous_scoring_class,
           lag(is_active,1) over (partition by player_name order by current_season) as previous_active_class
    from players
    where current_season <= 2021
),
  with_indicators as(
      select *,
             case when scoring_class <> previous_scoring_class then 1
                  when is_active <> previous_active_class then 1
                  else 0
                  end as change_indicator
      from with_previous
  ),
  with_streaks as(
      select *,
             sum(change_indicator) over(partition by player_name order by current_season) as streak_identifier
      from with_indicators
  )
select player_name,
       scoring_class,
       is_active,
       min(current_season) as start_season,
       max(current_season) as end_season,
       2021 as current_season
    from with_streaks
    group by player_name,streak_identifier, is_active, scoring_class
    order by player_name,streak_identifier
;

with last_season_scd as(
    select * from player_scd
    where current_season = 2021
    and end_season = 2021
),
 historical_scd as(
     select * from player_scd
    where current_season = 2021
    and end_season < 2021
 ),
 this_season_data as(
     select * from players
     where current_season = 2022
 )



