DROP table IF EXISTS [h9].[factSales_qualityLog]

-- Create the quality log table for dimSales
CREATE TABLE [h9].[factSales_qualityLog]
(
    [LogID] INT IDENTITY(1,1) PRIMARY KEY, -- Unique identifier for each log entry
    [BatchID] INT, -- The Batch ID for which the quality check was performed
    [CheckType] VARCHAR(255), -- The type of quality check performed (e.g., 'Records Existence', 'Field Emptiness Check', 'Merge Operation')
    [CheckResult] VARCHAR(50), -- The result of the check ('Pass' or 'Fail')
    [ErrorMessage] VARCHAR(MAX), -- Detailed error message in case of failure
    [LogDateTime] DATETIME DEFAULT(GETDATE()) -- The date and time when the log entry was created
);
GO



DROP PROCEDURE IF EXISTS [h9].[factSales_publish];
GO
CREATE PROCEDURE [h9].[factSales_publish]
@BatchId INT
AS
BEGIN
    SET NOCOUNT ON; -- Suppress  "X rows affected" message
-- Quality check: Ensure there are records to process for the given BatchId
    IF NOT EXISTS (SELECT 1 FROM [h9].[factSales_stg] WHERE [rowBatchKey] = @BatchId)
    BEGIN
        INSERT INTO [h9].[factSales_qualityLog] (BatchID, CheckType, CheckResult, ErrorMessage)
        VALUES (@BatchId, 'Records Existence', 'Fail', 'No records found for the provided BatchId in the staging table.');

        RAISERROR ('No records found for the provided BatchId in the staging table.', 16, 1);
        RETURN;
    END
    ELSE
    BEGIN
        INSERT INTO [h9].[factSales_qualityLog] (BatchID, CheckType, CheckResult)
        VALUES (@BatchId, 'Records Existence', 'Pass');
    END

    -- Check for empty receipt, unitsSold, idStore, or idProduct fields
    IF EXISTS (
        SELECT 1 
        FROM [h9].[factSales_stg] 
        WHERE [rowBatchKey] = @BatchId AND (ISNULL([receipt], '') = '' OR ISNULL([unitsSold], 0) = 0 OR ISNULL([idStore], '') = '' OR ISNULL([idProduct], '') = '')
    )
    BEGIN
        INSERT INTO [h9].[factSales_qualityLog] (BatchID, CheckType, CheckResult, ErrorMessage)
        VALUES (@BatchId, 'Field Emptiness Check', 'Fail', 'One or more records have empty receipt, unitsSold, idStore, or idProduct fields.');

        RAISERROR ('One or more records have empty receipt, unitsSold, idStore, or idProduct fields.', 16, 1);
        RETURN;
    END
    ELSE
    BEGIN
        INSERT INTO [h9].[factSales_qualityLog] (BatchID, CheckType, CheckResult)
        VALUES (@BatchId, 'Field Emptiness Check', 'Pass');
    END

    BEGIN TRY
        -- Merge-a gögnum úr staging töflu og yfir í target töfluna
        MERGE INTO [h9].[factSales] AS TRG
        USING (
            SELECT 
                [rowKey],
                [date], 
                [idStore] = ISNULL([idStore], 0),
                [idProduct] = ISNULL([idProduct], 0),
                [unitsSold] = ISNULL([unitsSold], 0),
                [receipt],
                [rowBatchKey]
            FROM 
                [h9].[factSales_stg]
            WHERE 
                [rowBatchKey] = @BatchId
        ) AS SRC
        ON SRC.rowKey = TRG.rowKey
        AND SRC.rowBatchKey = @BatchId
        WHEN MATCHED THEN
            UPDATE SET 
                TRG.[date] = SRC.[date],
                TRG.[idStore] = SRC.[idStore],
                TRG.[idProduct] = SRC.[idProduct],
                TRG.[unitsSold] = SRC.[unitsSold],
                TRG.[receipt] = SRC.[receipt],
                TRG.[rowModified] = GETUTCDATE()
        WHEN NOT MATCHED THEN
            INSERT ([rowKey], [date], [idStore], [idProduct], [unitsSold], [receipt], [rowBatchKey])
            VALUES (SRC.[rowKey], SRC.[date], SRC.[idStore], SRC.[idProduct], SRC.[unitsSold], SRC.[receipt], SRC.[rowBatchKey]);

        --  success message
        SELECT 'Procedure succeeded' AS [Status];

        -- Debugging: Check the affected rows
        SELECT @@ROWCOUNT AS 'RowsAffected';
    END TRY
    BEGIN CATCH
        --  error message
        SELECT ERROR_MESSAGE() AS [Error];
    END CATCH;
END
GO