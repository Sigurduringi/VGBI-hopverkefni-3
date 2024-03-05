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
    BEGIN TRY
        -- Log quality issues
        INSERT INTO [h9].[factInventory_qualityLog] (BatchId, RowKey, Issue)
        SELECT 
            @BatchId, 
            [rowKey],
            CASE 
                WHEN [idStore] IS NULL OR [idStore] < 0 THEN 'Invalid idStore'
                WHEN [idProduct] IS NULL OR [idProduct] < 0 THEN 'Invalid idProduct'
                WHEN [InStock] IS NULL OR [InStock] < 0 THEN 'Invalid InStock'
                ELSE NULL
            END
        FROM [h9].[factInventory_stg]
        WHERE [rowBatchKey] = @BatchId
        AND ([idStore] IS NULL OR [idStore] < 0 OR [idProduct] IS NULL OR [idProduct] < 0 OR [InStock] IS NULL OR [InStock] < 0);

        -- Merge data from staging table into target table, excluding rows with logged issues
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
                AND [idStore] >= 0
                AND [idProduct] >= 0
                AND [InStock] >= 0
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