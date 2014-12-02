/*
 * Daily Sales ETL Pipeline
 * Project #63 - Automated Nightly Batch Loads
 * SSIS, SQL Agent, SQL Server 2008
 * Created: 2012
 */

USE master;
GO
CREATE DATABASE SalesETL;
GO
USE SalesETL;
GO

-- Staging Tables
CREATE TABLE dbo.STG_Sales (
    StagingID INT IDENTITY(1,1) PRIMARY KEY,
    SourceSystem VARCHAR(50),
    TransactionNumber VARCHAR(30),
    TransactionDate DATETIME,
    BranchCode VARCHAR(10),
    CustomerCode VARCHAR(20),
    ProductCode VARCHAR(30),
    Quantity INT,
    UnitPrice DECIMAL(18,2),
    TotalAmount DECIMAL(18,2),
    LoadDate DATETIME DEFAULT GETDATE(),
    ProcessedFlag BIT DEFAULT 0,
    ErrorFlag BIT DEFAULT 0,
    ErrorMessage VARCHAR(500)
);

-- ETL Control Table
CREATE TABLE dbo.ETL_Control (
    ControlID INT IDENTITY(1,1) PRIMARY KEY,
    PackageName VARCHAR(100) NOT NULL,
    LastRunDate DATETIME,
    LastSuccessDate DATETIME,
    LastExtractedID INT DEFAULT 0,
    LastExtractedDate DATETIME,
    Status VARCHAR(20) DEFAULT 'Ready', -- Ready, Running, Success, Failed
    RecordsExtracted INT DEFAULT 0,
    RecordsLoaded INT DEFAULT 0,
    RecordsRejected INT DEFAULT 0
);

-- ETL Log Table
CREATE TABLE dbo.ETL_Log (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    PackageName VARCHAR(100) NOT NULL,
    StepName VARCHAR(100),
    StartTime DATETIME DEFAULT GETDATE(),
    EndTime DATETIME,
    Status VARCHAR(20), -- Started, Success, Failed
    RecordsProcessed INT DEFAULT 0,
    ErrorMessage VARCHAR(MAX),
    CreatedDate DATETIME DEFAULT GETDATE()
);

-- Error Quarantine Table
CREATE TABLE dbo.ETL_ErrorQuarantine (
    ErrorID INT IDENTITY(1,1) PRIMARY KEY,
    PackageName VARCHAR(100),
    SourceTable VARCHAR(100),
    SourceData VARCHAR(MAX),
    ErrorMessage VARCHAR(MAX),
    ErrorDate DATETIME DEFAULT GETDATE(),
    IsResolved BIT DEFAULT 0,
    ResolvedDate DATETIME,
    ResolvedBy VARCHAR(50)
);

-- Stored Procedure: Start ETL Package
CREATE PROCEDURE dbo.usp_StartETLPackage
    @PackageName VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if package is already running
    IF EXISTS (SELECT 1 FROM dbo.ETL_Control WHERE PackageName = @PackageName AND Status = 'Running')
    BEGIN
        RAISERROR('Package is already running', 16, 1);
        RETURN -1;
    END
    
    -- Update control table
    UPDATE dbo.ETL_Control
    SET Status = 'Running',
        LastRunDate = GETDATE(),
        RecordsExtracted = 0,
        RecordsLoaded = 0,
        RecordsRejected = 0
    WHERE PackageName = @PackageName;
    
    -- Log start
    INSERT INTO dbo.ETL_Log (PackageName, StepName, StartTime, Status)
    VALUES (@PackageName, 'Package Start', GETDATE(), 'Started');
    
    RETURN 0;
END
GO

-- Stored Procedure: End ETL Package
CREATE PROCEDURE dbo.usp_EndETLPackage
    @PackageName VARCHAR(100),
    @Status VARCHAR(20),
    @RecordsExtracted INT = 0,
    @RecordsLoaded INT = 0,
    @RecordsRejected INT = 0,
    @ErrorMessage VARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update control table
    UPDATE dbo.ETL_Control
    SET Status = @Status,
        RecordsExtracted = @RecordsExtracted,
        RecordsLoaded = @RecordsLoaded,
        RecordsRejected = @RecordsRejected,
        LastSuccessDate = CASE WHEN @Status = 'Success' THEN GETDATE() ELSE LastSuccessDate END
    WHERE PackageName = @PackageName;
    
    -- Log end
    INSERT INTO dbo.ETL_Log (PackageName, StepName, StartTime, EndTime, Status, RecordsProcessed, ErrorMessage)
    VALUES (@PackageName, 'Package End', GETDATE(), GETDATE(), @Status, @RecordsLoaded, @ErrorMessage);
    
    RETURN 0;
END
GO

-- Stored Procedure: Process Staging Data
CREATE PROCEDURE dbo.usp_ProcessStagingData
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRANSACTION;
    
    BEGIN TRY
        DECLARE @ProcessedCount INT = 0;
        DECLARE @ErrorCount INT = 0;
        
        -- Validate and mark errors
        UPDATE dbo.STG_Sales
        SET ErrorFlag = 1,
            ErrorMessage = 'Invalid data: ' + 
                CASE 
                    WHEN TransactionNumber IS NULL THEN 'Missing Transaction Number'
                    WHEN Quantity <= 0 THEN 'Invalid Quantity'
                    WHEN UnitPrice <= 0 THEN 'Invalid Unit Price'
                    ELSE 'Unknown Error'
                END
        WHERE ProcessedFlag = 0
        AND (TransactionNumber IS NULL OR Quantity <= 0 OR UnitPrice <= 0);
        
        SET @ErrorCount = @@ROWCOUNT;
        
        -- Move error records to quarantine
        INSERT INTO dbo.ETL_ErrorQuarantine (PackageName, SourceTable, SourceData, ErrorMessage)
        SELECT 'Daily Sales ETL', 'STG_Sales', 
               CAST(StagingID AS VARCHAR(10)) + '|' + ISNULL(TransactionNumber, 'NULL'),
               ErrorMessage
        FROM dbo.STG_Sales
        WHERE ErrorFlag = 1 AND ProcessedFlag = 0;
        
        -- Mark valid records as processed
        UPDATE dbo.STG_Sales
        SET ProcessedFlag = 1
        WHERE ErrorFlag = 0 AND ProcessedFlag = 0;
        
        SET @ProcessedCount = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        
        SELECT @ProcessedCount AS ProcessedRecords, @ErrorCount AS ErrorRecords;
        RETURN 0;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        DECLARE @Error NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@Error, 16, 1);
        RETURN -1;
    END CATCH
END
GO

-- Initialize Control Table
INSERT INTO dbo.ETL_Control (PackageName, Status)
VALUES ('Daily_Sales_ETL', 'Ready');

PRINT 'Sales ETL Database created successfully';
GO

/*
 * SSIS Package Design Notes:
 * 
 * Package: Daily_Sales_ETL.dtsx
 * 
 * Control Flow:
 * 1. Execute SQL Task: Call usp_StartETLPackage
 * 2. Data Flow Task: Extract from Source
 *    - OLE DB Source: Source database
 *    - Data Conversion: Convert data types
 *    - OLE DB Destination: STG_Sales table
 * 3. Execute SQL Task: Call usp_ProcessStagingData
 * 4. Data Flow Task: Load to Target
 *    - OLE DB Source: STG_Sales (ProcessedFlag=1, ErrorFlag=0)
 *    - Lookup Transformations: Validate foreign keys
 *    - OLE DB Destination: Target fact table
 * 5. Execute SQL Task: Call usp_EndETLPackage
 * 
 * Error Handling:
 * - Event Handler: OnError -> Log to ETL_Log
 * - Redirect error rows to ETL_ErrorQuarantine
 * 
 * Scheduling:
 * - SQL Server Agent Job: Daily at 2:00 AM
 * - Job Steps:
 *   1. Execute SSIS Package
 *   2. Send email notification on failure
 */
