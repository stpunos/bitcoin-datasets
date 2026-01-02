-- V1.1.2__Analytics_Schema.sql
-- Create ANALYTICS schema with sentiment analysis on news data

-- =====================================================
-- ANALYTICS SCHEMA CREATION
-- =====================================================

-- Create Analytics schema for derived/processed data
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- =====================================================
-- NEWS SENTIMENT TABLE
-- =====================================================

-- Table to store sentiment analysis results for each news article
CREATE OR REPLACE TABLE ANALYTICS.NEWS_SENTIMENT (
    ID NUMBER,                              -- News article ID (FK to COINDESK.NEWS)
    PUBLISHED_ON NUMBER,                    -- Timestamp of publication
    PUBLISHED_DATETIME TIMESTAMP_TZ,        -- Converted datetime for easier querying
    TITLE STRING,                           -- News title
    SOURCE STRING,                          -- News source
    BODY STRING,                            -- Full article text
    -- Sentiment scores from Cortex
    SENTIMENT_SCORE FLOAT,                  -- Overall sentiment (-1 to 1)
    SENTIMENT_LABEL STRING,                 -- POSITIVE, NEUTRAL, NEGATIVE
    TITLE_SENTIMENT_SCORE FLOAT,           -- Sentiment of title only
    -- Metadata
    ANALYZED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ID)
);

-- =====================================================
-- FEAR & GREED NEWS INDEX - HOURLY
-- =====================================================

-- Hourly aggregated sentiment index (0-100 scale, like traditional F&G index)
-- 0-24: Extreme Fear, 25-44: Fear, 45-55: Neutral, 56-75: Greed, 76-100: Extreme Greed
CREATE OR REPLACE TABLE ANALYTICS.FNG_NEWS_INDEX_HOURLY (
    HOUR_START TIMESTAMP_TZ,               -- Start of the hour
    -- Article counts
    TOTAL_ARTICLES INTEGER,                -- Total articles in this hour
    POSITIVE_ARTICLES INTEGER,             -- Number of positive articles
    NEUTRAL_ARTICLES INTEGER,              -- Number of neutral articles
    NEGATIVE_ARTICLES INTEGER,             -- Number of negative articles
    -- Sentiment aggregations
    AVG_SENTIMENT_SCORE FLOAT,             -- Average sentiment score
    WEIGHTED_SENTIMENT_SCORE FLOAT,        -- Weighted by upvotes/engagement
    -- Fear & Greed Index (0-100)
    FNG_INDEX_VALUE FLOAT,                 -- 0-100 scale
    FNG_INDEX_LABEL STRING,                -- Extreme Fear, Fear, Neutral, Greed, Extreme Greed
    -- Metadata
    CALCULATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (HOUR_START)
);

-- =====================================================
-- FEAR & GREED NEWS INDEX - DAILY
-- =====================================================

-- Daily aggregated sentiment index
CREATE OR REPLACE TABLE ANALYTICS.FNG_NEWS_INDEX_DAILY (
    DATE DATE,                             -- Date (UTC)
    -- Article counts
    TOTAL_ARTICLES INTEGER,                -- Total articles for the day
    POSITIVE_ARTICLES INTEGER,             -- Number of positive articles
    NEUTRAL_ARTICLES INTEGER,              -- Number of neutral articles
    NEGATIVE_ARTICLES INTEGER,             -- Number of negative articles
    -- Sentiment aggregations
    AVG_SENTIMENT_SCORE FLOAT,             -- Average sentiment score
    WEIGHTED_SENTIMENT_SCORE FLOAT,        -- Weighted by upvotes/engagement
    MIN_SENTIMENT_SCORE FLOAT,             -- Most negative score
    MAX_SENTIMENT_SCORE FLOAT,             -- Most positive score
    STDDEV_SENTIMENT_SCORE FLOAT,          -- Volatility of sentiment
    -- Fear & Greed Index (0-100)
    FNG_INDEX_VALUE FLOAT,                 -- 0-100 scale
    FNG_INDEX_LABEL STRING,                -- Extreme Fear, Fear, Neutral, Greed, Extreme Greed
    -- Daily change
    FNG_INDEX_CHANGE FLOAT,                -- Change from previous day
    FNG_INDEX_CHANGE_PCT FLOAT,            -- Percentage change from previous day
    -- Metadata
    CALCULATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (DATE)
);

-- =====================================================
-- STREAM ON NEWS TABLE
-- =====================================================

-- Stream to capture new inserts into COINDESK.NEWS
-- This enables continuous processing of news articles
CREATE OR REPLACE STREAM ANALYTICS.NEWS_STREAM 
ON TABLE COINDESK.NEWS
APPEND_ONLY = TRUE;  -- Only capture INSERTs (not UPDATEs/DELETEs)

-- =====================================================
-- STORED PROCEDURES FOR SENTIMENT ANALYSIS
-- =====================================================

-- Procedure 1: Analyze sentiment for new news articles using Cortex
CREATE OR REPLACE PROCEDURE ANALYTICS.ANALYZE_NEWS_SENTIMENT()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_processed INT DEFAULT 0;
BEGIN
    -- Insert new articles with sentiment analysis
    INSERT INTO ANALYTICS.NEWS_SENTIMENT (
        ID,
        PUBLISHED_ON,
        PUBLISHED_DATETIME,
        TITLE,
        SOURCE,
        BODY,
        SENTIMENT_SCORE,
        SENTIMENT_LABEL,
        TITLE_SENTIMENT_SCORE
    )
    SELECT 
        n.ID,
        n.PUBLISHED_ON,
        TO_TIMESTAMP(n.PUBLISHED_ON),
        n.TITLE,
        n.SOURCE,
        n.BODY,
        -- Cortex sentiment analysis on body text
        SNOWFLAKE.CORTEX.SENTIMENT(n.BODY),
        -- Classify sentiment into labels
        CASE 
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(n.BODY) >= 0.3 THEN 'POSITIVE'
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(n.BODY) <= -0.3 THEN 'NEGATIVE'
            ELSE 'NEUTRAL'
        END,
        -- Sentiment of title only
        SNOWFLAKE.CORTEX.SENTIMENT(n.TITLE)
    FROM ANALYTICS.NEWS_STREAM n
    WHERE METADATA$ACTION = 'INSERT'
    AND METADATA$ISUPDATE = FALSE;
    
    rows_processed := SQLROWCOUNT;
    
    RETURN 'Processed ' || rows_processed || ' news articles';
END;
$$;

-- Procedure 2: Calculate hourly Fear & Greed Index
CREATE OR REPLACE PROCEDURE ANALYTICS.CALCULATE_HOURLY_FNG_INDEX()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_calculated INT DEFAULT 0;
BEGIN
    -- Calculate hourly aggregations
    MERGE INTO ANALYTICS.FNG_NEWS_INDEX_HOURLY AS target
    USING (
        SELECT 
            DATE_TRUNC('HOUR', PUBLISHED_DATETIME) AS HOUR_START,
            COUNT(*) AS TOTAL_ARTICLES,
            SUM(CASE WHEN SENTIMENT_LABEL = 'POSITIVE' THEN 1 ELSE 0 END) AS POSITIVE_ARTICLES,
            SUM(CASE WHEN SENTIMENT_LABEL = 'NEUTRAL' THEN 1 ELSE 0 END) AS NEUTRAL_ARTICLES,
            SUM(CASE WHEN SENTIMENT_LABEL = 'NEGATIVE' THEN 1 ELSE 0 END) AS NEGATIVE_ARTICLES,
            AVG(SENTIMENT_SCORE) AS AVG_SENTIMENT_SCORE,
            -- Convert sentiment score (-1 to 1) to Fear & Greed scale (0 to 100)
            -- Formula: ((sentiment + 1) / 2) * 100
            ((AVG(SENTIMENT_SCORE) + 1) / 2) * 100 AS FNG_INDEX_VALUE
        FROM ANALYTICS.NEWS_SENTIMENT
        WHERE PUBLISHED_DATETIME >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
        GROUP BY DATE_TRUNC('HOUR', PUBLISHED_DATETIME)
    ) AS source
    ON target.HOUR_START = source.HOUR_START
    WHEN MATCHED THEN UPDATE SET
        target.TOTAL_ARTICLES = source.TOTAL_ARTICLES,
        target.POSITIVE_ARTICLES = source.POSITIVE_ARTICLES,
        target.NEUTRAL_ARTICLES = source.NEUTRAL_ARTICLES,
        target.NEGATIVE_ARTICLES = source.NEGATIVE_ARTICLES,
        target.AVG_SENTIMENT_SCORE = source.AVG_SENTIMENT_SCORE,
        target.FNG_INDEX_VALUE = source.FNG_INDEX_VALUE,
        target.FNG_INDEX_LABEL = CASE
            WHEN source.FNG_INDEX_VALUE <= 24 THEN 'EXTREME FEAR'
            WHEN source.FNG_INDEX_VALUE <= 44 THEN 'FEAR'
            WHEN source.FNG_INDEX_VALUE <= 55 THEN 'NEUTRAL'
            WHEN source.FNG_INDEX_VALUE <= 75 THEN 'GREED'
            ELSE 'EXTREME GREED'
        END,
        target.CALCULATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        HOUR_START,
        TOTAL_ARTICLES,
        POSITIVE_ARTICLES,
        NEUTRAL_ARTICLES,
        NEGATIVE_ARTICLES,
        AVG_SENTIMENT_SCORE,
        FNG_INDEX_VALUE,
        FNG_INDEX_LABEL
    ) VALUES (
        source.HOUR_START,
        source.TOTAL_ARTICLES,
        source.POSITIVE_ARTICLES,
        source.NEUTRAL_ARTICLES,
        source.NEGATIVE_ARTICLES,
        source.AVG_SENTIMENT_SCORE,
        source.FNG_INDEX_VALUE,
        CASE
            WHEN source.FNG_INDEX_VALUE <= 24 THEN 'EXTREME FEAR'
            WHEN source.FNG_INDEX_VALUE <= 44 THEN 'FEAR'
            WHEN source.FNG_INDEX_VALUE <= 55 THEN 'NEUTRAL'
            WHEN source.FNG_INDEX_VALUE <= 75 THEN 'GREED'
            ELSE 'EXTREME GREED'
        END
    );
    
    rows_calculated := SQLROWCOUNT;
    
    RETURN 'Calculated hourly F&G index for ' || rows_calculated || ' hours';
END;
$$;

-- Procedure 3: Calculate daily Fear & Greed Index
CREATE OR REPLACE PROCEDURE ANALYTICS.CALCULATE_DAILY_FNG_INDEX()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_calculated INT DEFAULT 0;
BEGIN
    -- Calculate daily aggregations with change metrics
    MERGE INTO ANALYTICS.FNG_NEWS_INDEX_DAILY AS target
    USING (
        WITH daily_stats AS (
            SELECT 
                DATE_TRUNC('DAY', PUBLISHED_DATETIME)::DATE AS DATE,
                COUNT(*) AS TOTAL_ARTICLES,
                SUM(CASE WHEN SENTIMENT_LABEL = 'POSITIVE' THEN 1 ELSE 0 END) AS POSITIVE_ARTICLES,
                SUM(CASE WHEN SENTIMENT_LABEL = 'NEUTRAL' THEN 1 ELSE 0 END) AS NEUTRAL_ARTICLES,
                SUM(CASE WHEN SENTIMENT_LABEL = 'NEGATIVE' THEN 1 ELSE 0 END) AS NEGATIVE_ARTICLES,
                AVG(SENTIMENT_SCORE) AS AVG_SENTIMENT_SCORE,
                MIN(SENTIMENT_SCORE) AS MIN_SENTIMENT_SCORE,
                MAX(SENTIMENT_SCORE) AS MAX_SENTIMENT_SCORE,
                STDDEV(SENTIMENT_SCORE) AS STDDEV_SENTIMENT_SCORE,
                ((AVG(SENTIMENT_SCORE) + 1) / 2) * 100 AS FNG_INDEX_VALUE
            FROM ANALYTICS.NEWS_SENTIMENT
            WHERE PUBLISHED_DATETIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
            GROUP BY DATE_TRUNC('DAY', PUBLISHED_DATETIME)::DATE
        ),
        with_previous AS (
            SELECT 
                *,
                LAG(FNG_INDEX_VALUE) OVER (ORDER BY DATE) AS prev_fng_value
            FROM daily_stats
        )
        SELECT 
            *,
            CASE 
                WHEN prev_fng_value IS NOT NULL 
                THEN FNG_INDEX_VALUE - prev_fng_value 
                ELSE NULL 
            END AS FNG_INDEX_CHANGE,
            CASE 
                WHEN prev_fng_value IS NOT NULL AND prev_fng_value != 0
                THEN ((FNG_INDEX_VALUE - prev_fng_value) / prev_fng_value) * 100
                ELSE NULL 
            END AS FNG_INDEX_CHANGE_PCT
        FROM with_previous
    ) AS source
    ON target.DATE = source.DATE
    WHEN MATCHED THEN UPDATE SET
        target.TOTAL_ARTICLES = source.TOTAL_ARTICLES,
        target.POSITIVE_ARTICLES = source.POSITIVE_ARTICLES,
        target.NEUTRAL_ARTICLES = source.NEUTRAL_ARTICLES,
        target.NEGATIVE_ARTICLES = source.NEGATIVE_ARTICLES,
        target.AVG_SENTIMENT_SCORE = source.AVG_SENTIMENT_SCORE,
        target.MIN_SENTIMENT_SCORE = source.MIN_SENTIMENT_SCORE,
        target.MAX_SENTIMENT_SCORE = source.MAX_SENTIMENT_SCORE,
        target.STDDEV_SENTIMENT_SCORE = source.STDDEV_SENTIMENT_SCORE,
        target.FNG_INDEX_VALUE = source.FNG_INDEX_VALUE,
        target.FNG_INDEX_LABEL = CASE
            WHEN source.FNG_INDEX_VALUE <= 24 THEN 'EXTREME FEAR'
            WHEN source.FNG_INDEX_VALUE <= 44 THEN 'FEAR'
            WHEN source.FNG_INDEX_VALUE <= 55 THEN 'NEUTRAL'
            WHEN source.FNG_INDEX_VALUE <= 75 THEN 'GREED'
            ELSE 'EXTREME GREED'
        END,
        target.FNG_INDEX_CHANGE = source.FNG_INDEX_CHANGE,
        target.FNG_INDEX_CHANGE_PCT = source.FNG_INDEX_CHANGE_PCT,
        target.CALCULATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        DATE,
        TOTAL_ARTICLES,
        POSITIVE_ARTICLES,
        NEUTRAL_ARTICLES,
        NEGATIVE_ARTICLES,
        AVG_SENTIMENT_SCORE,
        MIN_SENTIMENT_SCORE,
        MAX_SENTIMENT_SCORE,
        STDDEV_SENTIMENT_SCORE,
        FNG_INDEX_VALUE,
        FNG_INDEX_LABEL,
        FNG_INDEX_CHANGE,
        FNG_INDEX_CHANGE_PCT
    ) VALUES (
        source.DATE,
        source.TOTAL_ARTICLES,
        source.POSITIVE_ARTICLES,
        source.NEUTRAL_ARTICLES,
        source.NEGATIVE_ARTICLES,
        source.AVG_SENTIMENT_SCORE,
        source.MIN_SENTIMENT_SCORE,
        source.MAX_SENTIMENT_SCORE,
        source.STDDEV_SENTIMENT_SCORE,
        source.FNG_INDEX_VALUE,
        CASE
            WHEN source.FNG_INDEX_VALUE <= 24 THEN 'EXTREME FEAR'
            WHEN source.FNG_INDEX_VALUE <= 44 THEN 'FEAR'
            WHEN source.FNG_INDEX_VALUE <= 55 THEN 'NEUTRAL'
            WHEN source.FNG_INDEX_VALUE <= 75 THEN 'GREED'
            ELSE 'EXTREME GREED'
        END,
        source.FNG_INDEX_CHANGE,
        source.FNG_INDEX_CHANGE_PCT
    );
    
    rows_calculated := SQLROWCOUNT;
    
    RETURN 'Calculated daily F&G index for ' || rows_calculated || ' days';
END;
$$;

-- =====================================================
-- TASK FOR AUTOMATED PROCESSING
-- =====================================================

-- Task to run sentiment analysis every 5 minutes
CREATE OR REPLACE TASK ANALYTICS.TASK_ANALYZE_NEWS_SENTIMENT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ANALYTICS.NEWS_STREAM')
AS
    CALL ANALYTICS.ANALYZE_NEWS_SENTIMENT();

-- Task to calculate hourly F&G index after sentiment analysis
CREATE OR REPLACE TASK ANALYTICS.TASK_CALCULATE_HOURLY_FNG
    WAREHOUSE = COMPUTE_WH
    AFTER ANALYTICS.TASK_ANALYZE_NEWS_SENTIMENT
AS
    CALL ANALYTICS.CALCULATE_HOURLY_FNG_INDEX();

-- Task to calculate daily F&G index once per day
CREATE OR REPLACE TASK ANALYTICS.TASK_CALCULATE_DAILY_FNG
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 1 * * * UTC'  -- Daily at 1 AM UTC
AS
    CALL ANALYTICS.CALCULATE_DAILY_FNG_INDEX();

-- =====================================================
-- ENABLE TASKS (commented out - enable manually)
-- =====================================================

ALTER TASK ANALYTICS.TASK_CALCULATE_DAILY_FNG RESUME;
ALTER TASK ANALYTICS.TASK_CALCULATE_HOURLY_FNG RESUME;
ALTER TASK ANALYTICS.TASK_ANALYZE_NEWS_SENTIMENT RESUME;

-- =====================================================
-- VIEWS FOR EASY QUERYING
-- =====================================================

-- View: Latest sentiment trends
CREATE OR REPLACE VIEW ANALYTICS.VW_LATEST_NEWS_SENTIMENT AS
SELECT 
    ns.ID,
    ns.PUBLISHED_DATETIME,
    ns.TITLE,
    ns.SOURCE,
    ns.SENTIMENT_SCORE,
    ns.SENTIMENT_LABEL,
    ns.TITLE_SENTIMENT_SCORE,
    n.URL,
    n.UPVOTES,
    n.DOWNVOTES
FROM ANALYTICS.NEWS_SENTIMENT ns
JOIN COINDESK.NEWS n ON ns.ID = n.ID
ORDER BY ns.PUBLISHED_DATETIME DESC;

-- View: Current F&G Index (latest hourly and daily)
CREATE OR REPLACE VIEW ANALYTICS.VW_CURRENT_FNG_INDEX AS
SELECT 
    'HOURLY' AS TIMEFRAME,
    h.HOUR_START AS TIMESTAMP,
    h.FNG_INDEX_VALUE,
    h.FNG_INDEX_LABEL,
    h.TOTAL_ARTICLES,
    h.POSITIVE_ARTICLES,
    h.NEUTRAL_ARTICLES,
    h.NEGATIVE_ARTICLES,
    NULL AS CHANGE_VALUE,
    NULL AS CHANGE_PCT
FROM ANALYTICS.FNG_NEWS_INDEX_HOURLY h
WHERE h.HOUR_START = (SELECT MAX(HOUR_START) FROM ANALYTICS.FNG_NEWS_INDEX_HOURLY)

UNION ALL

SELECT 
    'DAILY' AS TIMEFRAME,
    d.DATE::TIMESTAMP_TZ AS TIMESTAMP,
    d.FNG_INDEX_VALUE,
    d.FNG_INDEX_LABEL,
    d.TOTAL_ARTICLES,
    d.POSITIVE_ARTICLES,
    d.NEUTRAL_ARTICLES,
    d.NEGATIVE_ARTICLES,
    d.FNG_INDEX_CHANGE AS CHANGE_VALUE,
    d.FNG_INDEX_CHANGE_PCT AS CHANGE_PCT
FROM ANALYTICS.FNG_NEWS_INDEX_DAILY d
WHERE d.DATE = (SELECT MAX(DATE) FROM ANALYTICS.FNG_NEWS_INDEX_DAILY);

-- =====================================================
-- COMMENTS FOR DOCUMENTATION
-- =====================================================

COMMENT ON SCHEMA ANALYTICS IS 'Analytics schema for derived metrics including sentiment analysis and Fear & Greed index';

COMMENT ON TABLE ANALYTICS.NEWS_SENTIMENT IS 'Sentiment analysis results for news articles using Snowflake Cortex';
COMMENT ON TABLE ANALYTICS.FNG_NEWS_INDEX_HOURLY IS 'Hourly aggregated Fear & Greed index based on news sentiment (0-100 scale)';
COMMENT ON TABLE ANALYTICS.FNG_NEWS_INDEX_DAILY IS 'Daily aggregated Fear & Greed index based on news sentiment with trend analysis';

COMMENT ON STREAM ANALYTICS.NEWS_STREAM IS 'Stream capturing new inserts into COINDESK.NEWS for real-time processing';

COMMENT ON PROCEDURE ANALYTICS.ANALYZE_NEWS_SENTIMENT() IS 'Analyzes sentiment of new news articles using Snowflake Cortex ML';
COMMENT ON PROCEDURE ANALYTICS.CALCULATE_HOURLY_FNG_INDEX() IS 'Calculates hourly Fear & Greed index from news sentiment';
COMMENT ON PROCEDURE ANALYTICS.CALCULATE_DAILY_FNG_INDEX() IS 'Calculates daily Fear & Greed index with trend metrics';

COMMENT ON VIEW ANALYTICS.VW_LATEST_NEWS_SENTIMENT IS 'Latest news articles with sentiment analysis results';
COMMENT ON VIEW ANALYTICS.VW_CURRENT_FNG_INDEX IS 'Current Fear & Greed index values (hourly and daily)';
