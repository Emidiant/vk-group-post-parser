create table groups
(
    domain      varchar not null,
    name        varchar,
    description varchar,
    "offset"    integer,
    type        varchar,
    allow       boolean default true,
    last_post_timestamp integer
);
