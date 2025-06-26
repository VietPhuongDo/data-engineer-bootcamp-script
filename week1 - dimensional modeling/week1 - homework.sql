-------------DDL actors--------------------
create type films as(
    film text,
    votes integer,
    rating real,
    filmid text
                    );
create type quality_class as enum(
    'star',
    'good',
    'average',
    'bad'
    );

create table actors(
    actor_id text,
    actor_name text,
    films films[],
    quality_class quality_class,
    is_active boolean,
    current_year integer
);

-- drop table actors;

-----------------Cumulative genearion query for each year--------------------
insert into actors
with previous_year as(
    select *
    from actors where current_year = 1981
),
this_year as(
select actorid,
       actor,
       year,
       array_agg(row(film,votes,rating,filmid)::films) as films
    from actor_films
    where year = 1982
    group by actorid, actor, year
),
combined as(
    select coalesce(t.actorid,p.actor_id) as actor_id,
       coalesce(t.actor,p.actor_name) as actor_name,
       case when p.films is null then t.films
            when t.films is not null then p.films || t.films
            else p.films
        end as films,
        case when t.year is null then false
                    else true
            end as is_active,
        coalesce(t.year,p.current_year+1) as current_year
from previous_year p
full outer join this_year t
on p.actor_name = t.actor
)
select
    actor_id,
    actor_name,
    min(films) as films,
    case when avg((f).rating) > 8 then 'star'::quality_class
         when avg((f).rating) > 7 then 'good'::quality_class
         when avg((f).rating) > 6 then 'average'::quality_class
         else 'bad'::quality_class
         end as quality_class,
         is_active,
         current_year
from combined,
     unnest(films) as f
group by actor_id, actor_name,is_active,current_year;

----------DDL for actors_history_scd--------------------
create table actors_history_scd(
    actor_id text,
    actor_name text,
    quality_class quality_class,
    is_active boolean,
    start_year INTEGER,
    end_year INTEGER,
    current_year INTEGER,
    primary key (actor_id,actor_name,start_year)
);

-----------------Backfill query for actors_history_scd----------------------

insert into actors_history_scd
with with_previous as (
select
    actor_id,
    actor_name,
    quality_class,
    is_active,
    lag(quality_class,1) over (partition by actor_id,actor_name order by current_year) as previous_quality_class,
    lag(is_active,1) over (partition by actor_id,actor_name order by current_year) as previous_active_class,
    current_year
from actors
where current_year <= 1981
),
with_indicator as(
    select *,
       case when quality_class <> previous_quality_class then 1
            when is_active <> previous_active_class then 1
            else 0
        end as indicator
    from with_previous
),
with_streaks as(
    select *,
       sum(indicator) over (partition by actor_id,actor_name order by current_year) as streak_indicator
from with_indicator
)
select actor_id,
       actor_name,
       quality_class,
       is_active,
       min(current_year) as start_year,
       max(current_year) as end_year,
       1981 as current_year
from with_streaks
group by actor_id, actor_name,streak_indicator, quality_class, is_active
order by actor_id, actor_name,streak_indicator;

---------------------Incremental query for actors_history_scd---------------------------
with historical_actor_scd as(
    select actor_id,
           actor_name,
           quality_class,
           is_active,
           start_year,end_year
    from actors_history_scd
    where current_year = 1981
    and end_year < 1981
),
last_year_scd as(
select actor_id,
           actor_name,
           quality_class,
           is_active,
           start_year,end_year
    from actors_history_scd
    where current_year = 1981
    and end_year = 1981
),
this_year_data as(
    select *
    from actors
    where current_year = 1982
),
---------unchanged record-----------
unchanged_record as(
    select t.actor_id,
       t.actor_name,
       t.quality_class,
       t.is_active,
       l.start_year,
       t.current_year as end_year
from this_year_data as t
join last_year_scd as l
on l.actor_name = t.actor_name and l.actor_id = t.actor_id
where l.is_active = t.is_active
and l.quality_class = t.quality_class
),
--------changed record-------------
changed_records as(
    select t.actor_id,
       t.actor_name,
       unnest(array[
           ----history data,from history to last year-----
           row(
               l.quality_class,
               l.is_active,
               l.start_year,
               l.end_year
               )::scd_actor_type,
           ---data in new year,in only new year-----
           row(
               t.quality_class,
               t.is_active,
               t.current_year,
               t.current_year
               )::scd_actor_type
           ]) as records
from this_year_data as t
join last_year_scd as l
on l.actor_name = t.actor_name and l.actor_id = t.actor_id
where l.is_active <> t.is_active
or l.quality_class <> t.quality_class
),
unnest_change_records as(
    select actor_id,
       actor_name,
       (records::scd_actor_type).quality_class as quality_class,
       (records::scd_actor_type).is_active as is_active,
       (records::scd_actor_type).start_year as start_year,
       (records::scd_actor_type).end_year as end_year
from changed_records
),
------new actor who joined in 1982 as newbie----------
new_records as(
    select t.actor_id,
       t.actor_name,
       t.quality_class,
       t.is_active,
       t.current_year as start_year,
       t.current_year as end_year
from this_year_data as t
left join last_year_scd as l
on l.actor_name = t.actor_name and l.actor_id = t.actor_id
where l.actor_name is null and l.actor_id is null
)
select * from historical_actor_scd
union
select * from unchanged_record
union
select * from unnest_change_records
union
select * from new_records
;






create type scd_actor_type as(
    quality_class quality_class,
    is_active boolean,
    start_year integer,
    end_year integer
                             )