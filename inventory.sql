CREATE TABLE [h9].[factInventory_qualityLog]
(
    [LogID] INT IDENTITY(1,1) PRIMARY KEY,
    [BatchID] INT,
    [CheckType] VARCHAR(255),
    [CheckResult] VARCHAR(50),
    [ErrorMessage] VARCHAR(MAX),
    [CheckTimestamp] DATETIME2 DEFAULT GETUTCDATE()
);



DROP PROCEDURE IF EXISTS [h9].[factInventory_publish];
GO
CREATE PROCEDURE [h9].[factInventory_publish]
@BatchId INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Quality check: Ensure there are records to process for the given BatchId
    IF NOT EXISTS (SELECT 1 FROM [h9].[factInventory_stg] WHERE [rowBatchKey] = @BatchId)
    BEGIN
        INSERT INTO [h9].[factInventory_qualityLog] (BatchID, CheckType, CheckResult, ErrorMessage)
        VALUES (@BatchId, 'Records Existence', 'Fail', 'No records found for the provided BatchId in the staging table.');

        RAISERROR ('No records found for the provided BatchId in the staging table.', 16, 1);
        RETURN;
    END
    ELSE
    BEGIN
        INSERT INTO [h9].[factInventory_qualityLog] (BatchID, CheckType, CheckResult)
        VALUES (@BatchId, 'Records Existence', 'Pass');
    END

    -- Quality check: Ensure there are no null or empty idStore or idProduct fields
    IF EXISTS (
        SELECT 1 
        FROM [h9].[factInventory_stg] 
        WHERE ([rowBatchKey] = @BatchId AND (ISNULL([idStore], '') = '' OR ISNULL([idProduct], '') = ''))
    )
    BEGIN
        INSERT INTO [h9].[factInventory_qualityLog] (BatchID, CheckType, CheckResult, ErrorMessage)
        VALUES (@BatchId, 'Null or Empty Checks', 'Fail', 'idStore or idProduct contains null or empty values in the staging table for the provided BatchId.');

        RAISERROR ('idStore or idProduct contains null or empty values in the staging table for the provided BatchId.', 16, 1);
        RETURN;
    END
    ELSE
    BEGIN
        INSERT INTO [h9].[factInventory_qualityLog] (BatchID, CheckType, CheckResult)
        VALUES (@BatchId, 'Null or Empty Checks', 'Pass');
    END

    BEGIN TRY
        -- Merge data from staging table into target table
        MERGE INTO [h9].[factInventory] AS TRG
        USING (
            SELECT 
                [rowKey],
                [idStore] = ISNULL([idStore], 0),
                [idProduct] = ISNULL([idProduct], 0),
                [InStock] = ISNULL([InStock], 0),
                [rowBatchKey]
            FROM 
                [h9].[factInventory_stg]
            WHERE 
                [rowBatchKey] = @BatchId
        ) AS SRC
        ON SRC.rowKey = TRG.rowKey
        AND SRC.rowBatchKey = @BatchId
        WHEN MATCHED THEN
            UPDATE SET 
                TRG.[idStore] = SRC.[idStore],
                TRG.[idProduct] = SRC.[idProduct],
                TRG.[InStock] = SRC.[InStock],
                TRG.[rowModified] = GETUTCDATE()
        WHEN NOT MATCHED THEN
            INSERT ([rowKey], [idStore], [idProduct], [InStock], [rowBatchKey])
            VALUES (SRC.[rowKey], SRC.[idStore], SRC.[idProduct], SRC.[InStock], SRC.[rowBatchKey]);

        -- Success message
        SELECT 'Procedure succeeded' AS [Status];
    END TRY
    BEGIN CATCH
        -- Error message
        SELECT ERROR_MESSAGE() AS [Error];
    END CATCH;
END
GO