import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events cascade"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs cascade"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay cascade"
user_table_drop = "DROP TABLE IF EXISTS dim_user cascade"
song_table_drop = "DROP TABLE IF EXISTS dim_song cascade"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist cascade"
time_table_drop = "DROP TABLE IF EXISTS dim_time cascade"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender CHAR(1),
    itemInSession BIGINT,
    lastName TEXT,
    length FLOAT,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration TEXT,
    sessionId BIGINT,
    song TEXT,
    status INT,
    ts BIGINT,
    userAgent TEXT,
    userId BIGINT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id TEXT,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location TEXT,
    artist_name TEXT,
    song_id TEXT,
    title TEXT,
    duration FLOAT,
    year INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay (
    songplay_id BIGINT PRIMARY KEY IDENTITY(1,1) NOT NULL,
    start_time DATETIME NOT NULL,
    user_id BIGINT NOT NULL,
    level TEXT NOT NULL,
    song_id TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    session_id BIGINT NOT NULL,
    location TEXT,
    user_agent TEXT NOT NULL
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user (
    user_id BIGINT PRIMARY KEY NOT NULL,
    first_name TEXT,
    last_name TEXT,
    gender CHAR(1),
    level TEXT
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song (
    song_id TEXT PRIMARY KEY NOT NULL,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    year INT,
    duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id TEXT PRIMARY KEY,
    name TEXT,
    location TEXT,
    latitude FLOAT,
    longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
    start_time DATETIME PRIMARY KEY NOT NULL,
    hour SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    week SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    weekday BOOLEAN NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    region 'us-west-2'
    iam_role {}
    json {}
""").format(
    config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'), 
    config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    region 'us-west-2'
    iam_role {}
    json 'auto'
""").format(
    config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'ARN')
)


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT
    TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second',
    e.userId,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId,
    e.location,
    e.userAgent
FROM staging_events as e
JOIN staging_songs as s
    ON s.artist_name = e.artist
    AND s.title = e.song
    AND s.duration = e.length
WHERE e.page = 'NextSong'
AND e.userId IS NOT NULL
AND s.song_id IS NOT NULL
AND s.artist_id IS NOT NULL
AND e.sessionId IS NOT NULL
AND e.userAgent IS NOT NULL
AND e.artist IS NOT NULL
AND e.song IS NOT NULL
AND e.ts IS NOT NULL
""")

user_table_insert = ("""
INSERT INTO dim_user (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT
    userId,
    firstName,
    lastName,
    gender,
    level
FROM staging_events as e
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO dim_song (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT 
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs as s
WHERE song_id IS NOT NULL
AND title IS NOT NULL
AND artist_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO dim_artist (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs as s
WHERE artist_id IS NOT NULL
AND artist_name IS NOT NULL
group by 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
""")

time_table_insert = ("""
INSERT INTO dim_time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT
    TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second',
    EXTRACT(hour FROM (TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second')),
    EXTRACT(day FROM (TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second')),
    EXTRACT(week FROM (TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second')),
    EXTRACT(month FROM (TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second')),
    EXTRACT(year FROM (TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second')),
    CASE WHEN EXTRACT(DOW FROM (TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second')) IN (6,7) THEN false ELSE true END
FROM staging_events as e
WHERE e.ts IS NOT NULL
group by
    TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second',
    EXTRACT(hour FROM (TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second')),
    EXTRACT(day FROM (TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second')),
    EXTRACT(week FROM (TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second')),
    EXTRACT(month FROM (TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second')),
    EXTRACT(year FROM (TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second')),
    CASE WHEN EXTRACT(DOW FROM (TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second')) IN (6,7) THEN false ELSE true END
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
