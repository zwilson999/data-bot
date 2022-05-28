create table hazardous_driving_areas
(
    geohash varchar(40),
    geohash_bounds varchar,
    latitude_sw decimal(8, 6),
    longitude_sw decimal(9, 6),
    latitude_ne decimal(8, 6),
    longitude_ne decimal(9, 6),
    location varchar,
    latitude decimal(8, 6),
    longitude decimal(9, 6),
    city varchar(100),
    county varchar(100),
    state varchar(50),
    country varchar(40),
    iso_3166_2 varchar(10),
    severity_score float,
    incidents_total int,
    update_date date,
    version varchar(5),
    inserted_date timestamp
);