create type vertex_type as enum(
    'player','team','game'
    );

create table vertices(
    identifier text,
    type vertex_type,
    properties json,
    primary key (identifier,type)
);

create type edge_type as enum(
    'plays_against',
    'shares_team',
    'plays_in',
    'plays_on'
    );

create table edges(
    --source vertex
    subject_identifier text,
    subject_type vertex_type,
    --destination vertex
    object_identifier text,
    object_type vertex_type,
    edge_type edge_type,
    properties json,
    primary key (subject_identifier,
                subject_type,
                object_identifier,
                object_type,
                edge_type)
);

------create vertices game first-----------------
insert into vertices
select
    game_id as identifier,
    'game'::vertex_type as type,
    json_build_object(
            'pts_home',pts_home,
            'pts_away',pts_away,
            'winning_team', case when home_team_wins = 1 then home_team_id
                                 else visitor_team_id end
    ) as properties
from games;

---------next is player's vertices--------------
insert into vertices
with players_agg as (select player_id                   as identifier,
                            max(player_name)            as player_name,
                            count(1)                    as number_of_games,
                            sum(pts)                    as total_points,
                            array_agg(distinct team_id) as teams
                     from game_details
                     group by player_id)
select identifier,
       'player'::vertex_type,
       json_build_object(
               'player_name',player_name,
               'number_of_games',number_of_games,
               'total_points',total_points,
               'teams',teams
       )
from players_agg;

-----final is teams_deduped because i load double row in this table
 insert into vertices
with deduped_teams as(
    select *,row_number() over (partition by team_id) as row_num
    from teams
)
select
    team_id as identifier,
    'team'::vertex_type as type,
    json_build_object(
            'abbreviation',abbreviation,
            'nickname',nickname,
            'city',city,
            'arena',arena,
            'year_founded',yearfounded
    )
from deduped_teams
where row_num = 1;

