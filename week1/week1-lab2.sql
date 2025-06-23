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
         current_season,
         scoring_class,
         is_active,
         lag(scoring_class,1) over (partition by player_name order by current_season) as previous_scoring_class,
         lag(is_active,1) over (partition by player_name order by current_season) as previous_active_class
  from players
  where current_season <= 2021
),
 with_indicator as (
     select *,
            case
                when scoring_class <> previous_scoring_class then 1
                when is_active <> previous_active_class then 1
                else 0
            end as change_indicator
     from with_previous
 ),
 with_streaks as (
     select *,
            sum(change_indicator) over (partition by player_name order by current_season) as streak_indicator
    from with_indicator
 )
    select player_name,
           scoring_class,
           is_active,
           min(current_season) as start_year,
           max(current_season) as end_year,
           2021 as current_season
    from with_streaks
    group by  player_name,streak_indicator, scoring_class, is_active
    order by player_name,streak_indicator
;

with last_season_scd as(
    select * from player_scd
    where current_season = 2021
    and end_season = 2021
),
 historical_scd as(
    select player_name,scoring_class,is_active,start_season,end_season
    from player_scd
    where current_season = 2021
    and end_season < 2021
 ),
 this_season_data as(
     select * from players
     where current_season = 2022
 ),
-- plus 1 on end_season on players not change data
 unchanged_record as(
     select ts.player_name,
            ts.scoring_class,
            ts.is_active,
            ls.start_season,
            ts.current_season as end_season
     from this_season_data as ts
     join last_season_scd ls
     on ts.player_name = ls.player_name
     where ts.scoring_class = ls.scoring_class and ts.is_active = ls.is_active
 ),
-- 2 cases for change record: players in previous season and change;players first in last season
 changed_record as (select ts.player_name,
                           unnest(
                                   array [
                                       row (
                                           ls.scoring_class,
                                           ls.is_active,
                                           ls.start_season,
                                           ls.end_season
                                           )::scd_players_type,
                                       row (
                                           ts.scoring_class,
                                           ts.is_active,
                                           ts.current_season,
                                           ts.current_season
                                           )::scd_players_type
                                       ]) as records
                    from this_season_data as ts
                    left join last_season_scd ls
                    on ts.player_name = ls.player_name
                    where (ts.scoring_class <> ls.scoring_class or ts.is_active <> ls.is_active)),
    unnested_changes_records as (
        select player_name,
               (records::scd_players_type).scoring_class,
               (records::scd_players_type).is_active,
               (records::scd_players_type).start_season,
               (records::scd_players_type).end_season
        from changed_record
    ),
    new_records as(
        select  ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ts.current_season as start_season,
                ts.current_season as end_season
        from this_season_data as ts
        left join  last_season_scd as ls
        on ts.player_name = ls.player_name
        where ls.player_name is null
    )


select * from historical_scd
union all
select * from unchanged_record
union all
select * from unnested_changes_records
union all
select * from new_records;

create type scd_players_type as(
            scoring_class scoring_class,
            is_active boolean,
            start_season integer,
            end_season integer
);




