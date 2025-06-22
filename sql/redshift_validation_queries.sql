-- 1. Verify Table Existence
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public';
-- Expected: users, songs, streaming, staging_users, staging_songs, staging_streaming

-- 2. Check Row Counts
SELECT 'users' AS table_name, COUNT(*) AS row_count FROM public.users
UNION ALL
SELECT 'songs' AS table_name, COUNT(*) AS row_count FROM public.songs
UNION ALL
SELECT 'streaming' AS table_name, COUNT(*) AS row_count FROM public.streaming;
-- Expected: Non-zero counts matching processed records (check S3 logs)

-- 3. Validate Users Data
SELECT user_id, user_name, user_age, user_country, created_at
FROM public.users
WHERE user_age < 0 OR user_id IS NULL OR user_name IS NULL
LIMIT 5;
-- Expected: Empty (cleaned data)

-- 4. Validate Songs Data
SELECT track_id, track_name, artists, duration_ms, track_genre
FROM public.songs
WHERE duration_ms < 0 OR track_id IS NULL
LIMIT 5;
-- Expected: Empty (cleaned data)

-- 5. Validate Streaming Data
SELECT user_id, song_id, listen_time
FROM public.streaming
WHERE user_id IS NULL OR song_id IS NULL OR listen_time IS NULL
LIMIT 5;
-- Expected: Empty (cleaned data)

-- 6. Verify Data Integrity (Joins)
SELECT s.user_id, s.song_id, s.listen_time, u.user_name, t.track_name
FROM public.streaming s
JOIN public.users u ON s.user_id = u.user_id
JOIN public.songs t ON s.song_id = t.track_id
LIMIT 10;
-- Expected: Valid user and track names

-- 7. Sample KPI Validation
SELECT t.track_genre, COUNT(*) AS total_listens
FROM public.streaming s
JOIN public.songs t ON s.song_id = t.track_id
GROUP BY t.track_genre
ORDER BY total_listens DESC
LIMIT 5;
-- Expected: Top genres matching S3 KPI outputs