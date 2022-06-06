create table posts
(
    id_increment        serial,
    domain              varchar not null,
    id                  bigint,
    from_id             bigint,
    owner_id            bigint,
    date                bigint,
    marked_as_ads       text,
    is_favorite         text,
    post_type           text,
    text                text,
    attachments         text,
    post_source         text,
    repost_post_id_from text,
    likes_count         double precision,
    views_count         double precision,
    reposts_count       double precision
);