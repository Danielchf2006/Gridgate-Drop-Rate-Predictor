
CREATE TABLE IF NOT EXISTS gi_interactive_queue (
    project_num             TEXT,
    request_status          TEXT,
    queue_date              TEXT,
    withdrawn_date          TEXT,
    done_date               TEXT,
    appl_in_service_date    TEXT,
    transmission_owner      TEXT,
    county                  TEXT,
    state                   TEXT,
    study_cycle             TEXT,
    study_group             TEXT,
    study_phase             TEXT,
    service_type            TEXT,
    poi_name                TEXT,
    summer_mw               REAL,
    winter_mw               REAL,
    fuel                    TEXT,
    generating_facility     TEXT,
    post_gia_status         TEXT,
    negotiated_in_svc_date  TEXT,
    dp1_eris_mw             REAL,
    dp1_nris_mw             REAL,
    dp2_eris_mw             REAL,
    dp2_nris_mw             REAL
);


SELECT
    county,                         -- geographic grouping
    state,
    COUNT(*)                        AS project_count,
    SUM(CASE WHEN request_status = 'Withdrawn' THEN 1 ELSE 0 END)
                                    AS withdrawn_count,
    ROUND(
        100.0 * SUM(CASE WHEN request_status = 'Withdrawn' THEN 1 ELSE 0 END)
        / COUNT(*), 2
    )                               AS withdrawal_rate_pct,
    AVG(summer_mw)                  AS avg_summer_mw,
    AVG(winter_mw)                  AS avg_winter_mw,
    SUM(summer_mw)                  AS total_summer_mw,
    SUM(winter_mw)                  AS total_winter_mw

FROM gi_interactive_queue
GROUP BY county, state
ORDER BY withdrawn_count DESC;


-- [MODEL_DATA] per-project rows with duration variables for both models
SELECT
    project_num,
    request_status,
    queue_date,
    transmission_owner,
    county,
    state,
    study_cycle,
    study_group,
    study_phase,
    service_type,
    summer_mw,
    winter_mw,
    fuel,
    generating_facility,
    withdrawn_date,
    done_date,
    post_gia_status,
    negotiated_in_svc_date,
    dp1_eris_mw,
    dp1_nris_mw,
    dp2_eris_mw,
    dp2_nris_mw,
    appl_in_service_date,

    -- DURATION VARIABLES
    -- How long the project has been / was in the queue (days)
    -- Active projects: days since queue entry to today
    -- Withdrawn/Done: days from queue entry to resolution date
    CAST(
        julianday(COALESCE(withdrawn_date, done_date, 'now'))
        - julianday(queue_date)
    AS INTEGER)                                             AS days_in_queue,

    -- How far in the future the applicant planned to be in service (days)
    -- Known upfront — longer horizon = higher drop risk
    CAST(
        julianday(appl_in_service_date) - julianday(queue_date)
    AS INTEGER)                                             AS planned_days_to_service,

    -- Gap between negotiated and applied-for service date (days)
    -- Positive = project was pushed back from original plan
    CAST(
        julianday(negotiated_in_svc_date) - julianday(appl_in_service_date)
    AS INTEGER)                                             AS service_date_slip

FROM gi_interactive_queue;

