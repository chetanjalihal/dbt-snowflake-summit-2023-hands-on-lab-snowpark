with lap_times as (
    select 
        {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id', 'lap']) }} as lap_times_id,
        race_id                                                                 as race_id,
        driver_id                                                               as driver_id,
        lap                                                                     as lap,
        driver_position                                                         as driver_position,
        lap_time_formatted                                                      as lap_time_formatted,
        official_laptime                                                        as official_laptime,
        lap_time_milliseconds                                                   as lap_time_milliseconds
    from {{ ref('stg_lap_times') }}
)
select * from lap_times
Our fct_lap_times is very similar to our staging file since this is clean demo data. In your real world data project your data will probably be messier and require extra filtering and aggregation prior to becoming a fact table exposed to your business users for utilizing.
Use the UI Build (buttom with hammer icon) to create the fct_lap_times model. dbt_build_fct_lap_times
Now we have both dim_races and fct_lap_times separately. Next we'll to join these to create lap trend analysis through the years.

Marts tables
Marts tables are where everything comes together to create our business-defined entities that have an identity and purpose. We'll be joining our dim_races and fct_lap_times together.

Create a new file under your marts folder called mrt_lap_times_years.sql.
Copy and Save the following code:
with lap_times as (
select * from {{ ref('fct_lap_times') }}
    ),
    races as (
    select * from {{ ref('dim_races') }}
    ),
    expanded_lap_times_by_year as (
        select 
            lap_times.race_id, 
            driver_id, 
            race_year,
            lap,
            lap_time_milliseconds 
        from lap_times
        left join races
            on lap_times.race_id = races.race_id
        where lap_time_milliseconds is not null 
    )
    select * from expanded_lap_times_by_year

