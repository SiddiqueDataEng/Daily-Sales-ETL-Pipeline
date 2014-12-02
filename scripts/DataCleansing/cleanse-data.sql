-- Data Cleansing Script for Daily-Sales-ETL-Pipeline
-- SQL Server 2008 R2 / 2012
-- Run before ETL load

USE SalesETL;
GO

-- Remove duplicates
WITH CTE_Duplicates AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY KeyColumn ORDER BY ModifiedDate DESC) AS RowNum
    FROM dbo.StagingTable
)
DELETE FROM CTE_Duplicates WHERE RowNum > 1;

-- Standardize formats
UPDATE dbo.StagingTable
SET PhoneNumber = REPLACE(REPLACE(REPLACE(PhoneNumber, '(', ''), ')', ''), '-', '')
WHERE PhoneNumber IS NOT NULL;

-- Fix data quality issues
UPDATE dbo.StagingTable
SET Email = LOWER(LTRIM(RTRIM(Email)))
WHERE Email IS NOT NULL;

-- Handle nulls
UPDATE dbo.StagingTable
SET Status = 'Unknown'
WHERE Status IS NULL OR Status = '';

-- Validate data
INSERT INTO dbo.DataQualityLog (TableName, IssueType, RecordCount, LogDate)
SELECT 'StagingTable', 'Invalid Email', COUNT(*), GETDATE()
FROM dbo.StagingTable
WHERE Email NOT LIKE '%@%.%';

PRINT 'Data cleansing completed';
GO
