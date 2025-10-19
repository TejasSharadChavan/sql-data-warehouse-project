-- exec bronze.load_bronze;
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    PRINT '------------------------------------------------------------';
    PRINT '>> Starting Bronze Layer Load Procedure...';
    PRINT '------------------------------------------------------------';

    DECLARE 
        @startTime DATETIME,
        @endTime DATETIME,
        @durationSec INT,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    ------------------------------------------------------------
    -- CRM Customer Info
    ------------------------------------------------------------
    BEGIN TRY
        SET @batch_start_time  = GETDATE();
        SET @startTime = GETDATE();
        PRINT '>> Loading CRM: Customer Info...';

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\sql-server-project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @endTime = GETDATE();
        SET @durationSec = DATEDIFF(SECOND, @startTime, @endTime);
        PRINT '>> CRM Customer Info loaded successfully in ' + CAST(@durationSec AS NVARCHAR(10)) + ' seconds.';
    END TRY
    BEGIN CATCH
        PRINT '>> Error loading CRM Customer Info: ' + ERROR_MESSAGE();
    END CATCH;


    ------------------------------------------------------------
    -- CRM Product Info
    ------------------------------------------------------------
    BEGIN TRY
        SET @startTime = GETDATE();
        PRINT '>> Loading CRM: Product Info...';

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\sql-server-project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @endTime = GETDATE();
        SET @durationSec = DATEDIFF(SECOND, @startTime, @endTime);
        PRINT '>> CRM Product Info loaded successfully in ' + CAST(@durationSec AS NVARCHAR(10)) + ' seconds.';
    END TRY
    BEGIN CATCH
        PRINT '>> Error loading CRM Product Info: ' + ERROR_MESSAGE();
    END CATCH;


    ------------------------------------------------------------
    -- CRM Sales Details
    ------------------------------------------------------------
    BEGIN TRY
        SET @startTime = GETDATE();
        PRINT '>> Loading CRM: Sales Details...';

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\sql-server-project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @endTime = GETDATE();
        SET @durationSec = DATEDIFF(SECOND, @startTime, @endTime);
        PRINT '>> CRM Sales Details loaded successfully in ' + CAST(@durationSec AS NVARCHAR(10)) + ' seconds.';
    END TRY
    BEGIN CATCH
        PRINT '>> Error loading CRM Sales Details: ' + ERROR_MESSAGE();
    END CATCH;


    ------------------------------------------------------------
    -- ERP Customer AZ12
    ------------------------------------------------------------
    BEGIN TRY
        SET @startTime = GETDATE();
        PRINT '>> Loading ERP: Customer AZ12...';

        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\sql-server-project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @endTime = GETDATE();
        SET @durationSec = DATEDIFF(SECOND, @startTime, @endTime);
        PRINT '>> ERP Customer AZ12 loaded successfully in ' + CAST(@durationSec AS NVARCHAR(10)) + ' seconds.';
    END TRY
    BEGIN CATCH
        PRINT '>> Error loading ERP Customer AZ12: ' + ERROR_MESSAGE();
    END CATCH;


    ------------------------------------------------------------
    -- ERP Location A101
    ------------------------------------------------------------
    BEGIN TRY
        SET @startTime = GETDATE();
        PRINT '>> Loading ERP: Location A101...';

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\sql-server-project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @endTime = GETDATE();
        SET @durationSec = DATEDIFF(SECOND, @startTime, @endTime);
        PRINT '>> ERP Location A101 loaded successfully in ' + CAST(@durationSec AS NVARCHAR(10)) + ' seconds.';
    END TRY
    BEGIN CATCH
        PRINT '>> Error loading ERP Location A101: ' + ERROR_MESSAGE();
    END CATCH;


    ------------------------------------------------------------
    -- ERP PX Category G1V2
    ------------------------------------------------------------
    BEGIN TRY
        SET @startTime = GETDATE();
        PRINT '>> Loading ERP: PX Category G1V2...';

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\sql-server-project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @endTime = GETDATE();
        SET @durationSec = DATEDIFF(SECOND, @startTime, @endTime);
        PRINT '>> ERP PX Category G1V2 loaded successfully in ' + CAST(@durationSec AS NVARCHAR(10)) + ' seconds.';

        SET @batch_end_time = GETDATE();
        SET @durationSec = DATEDIFF(SECOND, @batch_start_time, @batch_end_time);
        PRINT '>> TOTAL LOAD DURATION TIME: ' + CAST(@durationSec AS NVARCHAR(10)) + ' seconds.';
    END TRY
    BEGIN CATCH
        PRINT '>> Error loading ERP PX Category G1V2: ' + ERROR_MESSAGE();
    END CATCH;

    PRINT '------------------------------------------------------------';
    PRINT '>> Bronze Layer Load Completed Successfully!';
    PRINT '------------------------------------------------------------';
END;
GO

