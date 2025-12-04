CREATE TABLE IF NOT EXISTS bronze.etl_log (
    log_id SERIAL PRIMARY KEY,
    procedure_name VARCHAR(100),
    table_name VARCHAR(100),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration INTERVAL,
    duration_seconds NUMERIC,
    rows_loaded INTEGER,
    status VARCHAR(20),
    error_message TEXT
);

-- log tracking
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    table_start TIMESTAMP;
    table_duration INTERVAL;
    total_duration INTERVAL;
    row_count INTEGER;
BEGIN
    start_time := CLOCK_TIMESTAMP();
    
    -- CRM: cust_info
    table_start := CLOCK_TIMESTAMP();
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/Users/datasets/source_crm/cust_info.csv' -- within the container, the location of the file
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    
    GET DIAGNOSTICS row_count = ROW_COUNT;  -- Get number of loaded rows
    table_duration := CLOCK_TIMESTAMP() - table_start;
    
    INSERT INTO bronze.etl_log (procedure_name, table_name, start_time, end_time, duration, duration_seconds, rows_loaded, status)
    VALUES ('load_bronze', 'crm_cust_info', table_start, CLOCK_TIMESTAMP(), table_duration, 
            EXTRACT(EPOCH FROM table_duration), row_count, 'SUCCESS');
    
    -- CRM: prd_info
    table_start := CLOCK_TIMESTAMP();
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/Users/datasets/source_crm/prd_info.csv' -- within the container, the location of the file
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    
    GET DIAGNOSTICS row_count = ROW_COUNT;
    table_duration := CLOCK_TIMESTAMP() - table_start;
    
    INSERT INTO bronze.etl_log (procedure_name, table_name, start_time, end_time, duration, duration_seconds, rows_loaded, status)
    VALUES ('load_bronze', 'crm_prd_info', table_start, CLOCK_TIMESTAMP(), table_duration, 
            EXTRACT(EPOCH FROM table_duration), row_count, 'SUCCESS');
    
    -- CRM: sales_details
    table_start := CLOCK_TIMESTAMP();
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/Users/datasets/source_crm/sales_details.csv' -- within the container, the location of the file
    WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '0');
    
    GET DIAGNOSTICS row_count = ROW_COUNT;
    table_duration := CLOCK_TIMESTAMP() - table_start;
    
    INSERT INTO bronze.etl_log (procedure_name, table_name, start_time, end_time, duration, duration_seconds, rows_loaded, status)
    VALUES ('load_bronze', 'crm_sales_details', table_start, CLOCK_TIMESTAMP(), table_duration, 
            EXTRACT(EPOCH FROM table_duration), row_count, 'SUCCESS');
    
    -- ERP: cust_az12
    table_start := CLOCK_TIMESTAMP();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/Users/datasets/source_erp/CUST_AZ12.csv' -- within the container, the location of the file
    WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '0');
    
    GET DIAGNOSTICS row_count = ROW_COUNT;
    table_duration := CLOCK_TIMESTAMP() - table_start;
    
    INSERT INTO bronze.etl_log (procedure_name, table_name, start_time, end_time, duration, duration_seconds, rows_loaded, status)
    VALUES ('load_bronze', 'erp_cust_az12', table_start, CLOCK_TIMESTAMP(), table_duration, 
            EXTRACT(EPOCH FROM table_duration), row_count, 'SUCCESS');
    
    -- ERP: loc_a101
    table_start := CLOCK_TIMESTAMP();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/Users/datasets/source_erp/LOC_A101.csv' -- within the container, the location of the file
    WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '0');
    
    GET DIAGNOSTICS row_count = ROW_COUNT;
    table_duration := CLOCK_TIMESTAMP() - table_start;
    
    INSERT INTO bronze.etl_log (procedure_name, table_name, start_time, end_time, duration, duration_seconds, rows_loaded, status)
    VALUES ('load_bronze', 'erp_loc_a101', table_start, CLOCK_TIMESTAMP(), table_duration, 
            EXTRACT(EPOCH FROM table_duration), row_count, 'SUCCESS');
    
    -- ERP: px_cat_g1v2
    table_start := CLOCK_TIMESTAMP();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/Users/datasets/source_erp/PX_CAT_G1V2.csv' -- within the container, the location of the file
    WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '0');
    
    GET DIAGNOSTICS row_count = ROW_COUNT;
    table_duration := CLOCK_TIMESTAMP() - table_start;
    
    INSERT INTO bronze.etl_log (procedure_name, table_name, start_time, end_time, duration, duration_seconds, rows_loaded, status)
    VALUES ('load_bronze', 'erp_px_cat_g1v2', table_start, CLOCK_TIMESTAMP(), table_duration, 
            EXTRACT(EPOCH FROM table_duration), row_count, 'SUCCESS');
    
    -- total duration calc
    total_duration := CLOCK_TIMESTAMP() - start_time;
    RAISE NOTICE 'ETL completed in %', total_duration;
    
EXCEPTION
    WHEN OTHERS THEN
        -- log the error
        INSERT INTO bronze.etl_log (procedure_name, start_time, end_time, status, error_message)
        VALUES ('load_bronze', start_time, CLOCK_TIMESTAMP(), 'FAILED', SQLERRM);
        
        RAISE EXCEPTION 'ETL failed: %', SQLERRM;
END;
$$;

-- view log records
SELECT
    table_name,
    start_time,
    duration,
    duration_seconds || ' seconds' AS duration_sec,
    rows_loaded,
    status
FROM bronze.etl_log
ORDER BY log_id DESC
LIMIT 10;

-- run the procedure
CALL bronze.load_bronze();
