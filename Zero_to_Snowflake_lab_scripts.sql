

/*********************************************************/
/****  Zero to Snowflake                              ****/
/****  Hands on Lab Script                            ****/
/*********************************************************/


--   Setting the Context 
use role student_%;  
use warehouse WORKSHOP_WH;
Use database sf_workshop;
Use schema student_%;  

-- Loading Data
-- Create Table 

create or replace table trips  
(tripduration integer,
  starttime timestamp,
  stoptime timestamp,
  start_station_id integer,
  start_station_name string,
  start_station_latitude float,
  start_station_longitude float,
  end_station_id integer,
  end_station_name string,
  end_station_latitude float,
  end_station_longitude float,
  bikeid integer,
  membership_type string,
  usertype string,
  birth_year integer,
  gender integer);


--Create stage 
CREATE OR REPLACE STAGE citibike_trips URL = 's3://snowflake-workshop-lab/citibike-trips/';

-- Show and list stage

show stages;

list @citibike_trips;


-- Create file format 

CREATE OR REPLACE FILE FORMAT STD_CSV 
        TYPE = 'CSV' 
        COMPRESSION = 'AUTO' 
        FIELD_DELIMITER = ',' 
        RECORD_DELIMITER = '\n' 
        SKIP_HEADER = 0 
        FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
        TRIM_SPACE = FALSE 
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE 
        ESCAPE = 'NONE' 
        ESCAPE_UNENCLOSED_FIELD = '\134' 
        DATE_FORMAT = 'AUTO' 
        TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('');

show file formats;

-- Select from stage

select t.$1::number as duration
     , t.$2::timestamp as start_time
     , t.$3::timestamp as stop_time 
     , t.$4::number as start_station_id 
     , t.$5::varchar as start_station_name 
     , t.$6::float as start_station_latitude 
     , t.$7::float as start_station_longitude 
     , t.$8::number as end_station_id 
     , t.$9::varchar as end_station_name 
     , t.$10::float as end_station_latitude 
     , t.$11::float as end_station_longitude 
     , t.$12::number as bike_id 
     , t.$13::varchar as membership_type 
     , t.$14::varchar as usertype 
     , t.$15::number as birth_year 
     , t.$16::number as gender  
from @citibike_trips/trips_2013_0_0_0.csv.gz 
(file_format=>STD_CSV) 
t;


-- Loading Data 
--Load trips table with small warehouse

copy into trips from @citibike_trips
file_format=STD_CSV;


select * from trips limit 20;

-- Clear out for reload


truncate table trips;


select * from trips limit 20;


-- Reload trips table with XL warehouse
show warehouses;

copy into trips from @citibike_trips
file_format=STD_CSV;


select * from trips limit 20;


-- Working with Queries 

-- Trips by Day/Hour
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)"
from trips
group by 1 order by 1;

-- Query Cache

select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)"
from trips
group by 1 order by 1;

--Trips by Day of Week
select
    dayname(starttime) as "day of week",
    count(*) as "num trips"
from trips
group by 1 order by 2 desc;

-- User Defined Function 
create or replace function trip_distance(start_lat float, start_long float, end_lat float, end_long float)
  returns float 
  as 'select haversine(start_lat, start_long, end_lat, end_long)';

-- Hourly Trip Statistics
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)", 
avg(trip_distance(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)" 
from trips
group by 1 order by 1;

show functions;

show user functions;

create or replace function trip_distance(lat float, long float)
  returns GEOGRAPHY 
  as 'select st_makepoint(lat, long)';

select date_trunc('hour', starttime) as "date",
starttime,
stoptime,
tripduration as "duration (mins)", 
st_makepoint(start_station_latitude, start_station_longitude) as start_station_geo,
trip_distance(start_station_latitude, start_station_longitude) as star_geo
from trips
where starttime::date = '2018-02-08'
limit 100
;


show user functions;

-- User Defined Table Function

create or replace function trips_by_date(start_date date, end_date date)
    returns table(trip_date date
                 , number_of_trips number
                 , average_duration_minutes number(12,6)
                 , average_distance_km float)
    as
    $$
       select starttime::date as trip_date,
       count(*) as number_of_trips,
       avg(tripduration) as average_duration_minutes, 
       avg(trip_distance(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as average_distance_km
       from trips
       where starttime::date between start_date and end_date
       group by trip_date
    $$;
    
select * 
    from table(trips_by_date('2018-02-09'::date,'2018-02-20'::date))
    order by average_distance_km
;




-- Zero Copy Clone

create table trips_dev clone trips;

select
    dayname(starttime) as "day of week",
    count(*) as "num trips"
from trips_dev
group by 1 order by 2 desc;

-- Working with Semi-Structured Data
-- Create table with variant column

create table json_weather_data (v variant);

-- Create and list Stage

create stage nyc_weather
url = 's3://snowflake-workshop-lab/weather-nyc';


list Place_name_here;  --use the object view to place the name of the stage in the worksheet

-- Load weather data

copy into json_weather_data 
from @nyc_weather 
file_format = (type=json);


select * from json_weather_data limit 10;

-- Create view to query semi-structured data using SQL dot notation

create view json_weather_data_view as
select
  v:time::timestamp as observation_time,
  v:city.id::int as city_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:city.coord.lat::float as city_lat,
  v:city.coord.lon::float as city_lon,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:weather[0].main::string as weather,
  v:weather[0].description::string as weather_desc,
  v:weather[0].icon::string as weather_icon,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from json_weather_data
where city_id = 5128638;

-- Query semi structured data view

select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01' 
limit 20;

-- Join bike trips data to semi-structured weather data

select weather as conditions
    ,count(*) as num_trips
from trips 
left outer join json_weather_data_view
    on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;


-- Time Travel

-- Dropped table

drop table json_weather_data;

Select * from json_weather_data limit 10;

undrop table json_weather_data;

Select * from json_weather_data limit 10;


-- Undesired Update

update trips set start_station_name = 'oops';


select 
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


set query_id = 
(select query_id from 
table(information_schema.query_history_by_session (result_limit=>5)) 
where query_text like 'update%' order by start_time limit 1);


select $query_id;


create or replace table trips as
(select * from trips before (statement => $query_id));
        


select 
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;




