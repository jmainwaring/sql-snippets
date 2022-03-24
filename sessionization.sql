-- Sessionization

/* 
Challenge: given a table with user IDs and timestamps for each action on the website, group these actions into
user sessions/'flurries'. A new session starts if more than ten seconds goes by for the user without any new activity
*/

WITH surrounding_ts AS (
    SELECT
        * 
        , LAG(timestamp) OVER (PARTITION BY customer_id ORDER BY timestamp) AS prev_event_ts
        , LEAD(timestamp) OVER (PARTITION BY customer_id ORDER BY timestamp) AS next_event_ts
    FROM raw_events
),

time_diff AS (
    SELECT
        *
        , DATEDIFF(prev_ts, ts, ‘second’) AS time_since_prev_event
        , DATEDIFF(ts, next_ts, ‘second’) AS time_until_next_event
    FROM surrounding_ts
),

flurry_start_end AS ( 
    SELECT 
          user_id  
        , ts
        , CASE WHEN (time_since_prev_event is NULL OR time_since_prev_event > 10) THEN 1 ELSE 0 END AS is_flurry_start
        , CASE WHEN (time_until_next_event is NULL OR time_until_next_event > 10) THEN 1 ELSE 0 END AS is_flurry_end
    FROM time_diff
),

session_id_creation AS (
    SELECT
          user_id
        , ts
        , CASE WHEN is_flurry_start THEN ROW_NUMBER() OVER (ORDER BY ts) -> MD5(user_id || ts) ELSE NULL END AS initial_session_id
    FROM flurry_start_end
),

full_session_id_assignment AS (
    SELECT
          user_id
        , ts
        , CASE WHEN is_flurry_start THEN initial_session_id
               ELSE LAG(initial_session_id) IGNORE NULLS OVER (ORDER BY user_id, ts) END AS session_id
    FROM session_id_creation
)

SELECT *
FROM full_session_id_assignment;
