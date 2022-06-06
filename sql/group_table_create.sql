drop table if exists groups;

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

insert into groups ("domain", name, description, "offset", type, allow, last_post_timestamp)
values  ('kartiny_history','Картины с историей','',0,'painting',true,null),
        ('taina_kartiny','Тайна картины','',0,'painting',true,null),
        ('ppl_choice','People','',0,'photo',true,null),
        ('million_kartin','Прекрасные картины','',0,'painting',true,null),
        ('fairwindpage','Fairwind','',0,'photo',true,null),
        ('henri_cartier_bresson','Photography','',0,'photo',true,null),
        ('trita.plenka','Плёнка','',0,'photo',true,null),
        ('kartini_s_istoriei','Картины с историей 2','',0,'painting',true,null),
        ('talentpainters','Картины талантливых художников','',0,'painting',true,null),
        ('whrmedia','W H R','',0,'photo',true,null);