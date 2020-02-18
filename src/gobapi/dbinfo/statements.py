DB_STATEMENTS = {
    "activity": """
SELECT
    *
FROM
    (
    SELECT
        state,
        ROUND((EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM query_start)) / 60)::numeric::integer
            AS duration_minutes,
        CONCAT(SUBSTRING(query, 1, 25), '...') -- Do not expose query !!
    FROM
        pg_stat_activity
    WHERE
        state = 'active' AND
        backend_type = 'client backend'
    )
AS
    t
WHERE
    duration_minutes > 0
ORDER BY
    duration_minutes
DESC
""",
    "locks": """
SELECT
    locktype,
    mode
FROM
    pg_locks
WHERE NOT GRANTED;
"""
}
