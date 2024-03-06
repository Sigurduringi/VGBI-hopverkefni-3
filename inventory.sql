DROP TABLE if exists [h9].[factSales_stg]
go
CREATE TABLE [h9].[factSales_stg]
(
[id] [int] identity(1,1) not null
, [rowKey] [nvarchar](200)
, [date] [date]
, [idStore] [int] 
, [idProduct] [INT]
, [unitsSold] [smallint]
, [receipt] [nvarchar](200)
-- CTRL
, [rowBatchKey] [int] not null
, [rowCreated] [datetime] not null default getutcdate()
, CONSTRAINT [pk_factSales_stg] PRIMARY KEY CLUSTERED ([id])
);
go
-- sama og að setja unique key á [rowKey] því við ákveðum gildið í rowBatchKey
create unique index UIX_factSales_BatchId_rowKey on [h9].[factSales_stg]
([rowBatchKey],[rowKey]);
go

DROP TABLE if exists [h9].[factSales]
go
CREATE TABLE [h9].[factSales]
(
[id] [int] identity(1,1) not null
, [rowKey] [nvarchar](200)
, [date] [date]
, [idStore] [int] 
, [idProduct] [INT]
, [unitsSold] [smallint]
, [receipt] [nvarchar](200) 
-- CTRL
, [rowBatchKey] [int] not null
, [rowCreated] [datetime] not null default getutcdate()
, CONSTRAINT [pk_factSales] PRIMARY KEY CLUSTERED ([id])
);
go
-- ATH: rowBatchKey ekki lengur hluti af UIX
create unique index UIX_factSales_rowKey on [h9].[factSales] ([rowKey]);
go


drop procedure if exists [h9].[factSales_postprocess];
go
create procedure [h9].[factSales_postprocess]
@BatchId int
as
    TRUNCATE table [H9].[factSales_stg];
    SELECT dummyval = 2
    RETURN 1
go


DROP PROCEDURE IF EXISTS [h9].[factSales_publish];
GO
CREATE PROCEDURE [h9].[factSales_publish]
@BatchId INT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        -- Log quality issues for idStore, idProduct, unitsSold, and receipt
        INSERT INTO [h9].[factSales_qualityLog] (BatchId, RowKey, Issue)
        SELECT 
            @BatchId, 
            [rowKey],
            CASE 
                WHEN [idStore] IS NULL OR [idStore] < 0 THEN 'Invalid idStore'
                WHEN [idProduct] IS NULL OR [idProduct] < 0 THEN 'Invalid idProduct'
                WHEN [receipt] IS NULL OR LTRIM(RTRIM([receipt])) = '' THEN 'Invalid receipt'
                WHEN [unitsSold] IS NULL OR [unitsSold] < 0 THEN 'Possible return or invalid unitsSold'
                ELSE NULL
            END
        FROM [h9].[factSales_stg]
        WHERE [rowBatchKey] = @BatchId
        AND ([idStore] IS NULL OR [idStore] < 0 OR [idProduct] IS NULL OR [idProduct] < 0  OR [receipt] IS NULL OR LTRIM(RTRIM([receipt])) = '' or unitsSold is null or unitsSold < 0);

        -- Merge data from staging table into target table, excluding rows with quality issues
        MERGE INTO [h9].[factSales] AS TRG
        USING (
            SELECT 
                [rowKey],
                [date], 
                [idStore] = ISNULL([idStore], 0),
                [idProduct] = ISNULL([idProduct], 0),
                [unitsSold] = ISNULL([unitsSold], 0),
                [receipt] = ISNULL([receipt], 0),
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
                TRG.[receipt] = SRC.[receipt]
        WHEN NOT MATCHED THEN
            INSERT ([rowKey], [date], [idStore], [idProduct], [unitsSold], [receipt], [rowBatchKey])
            VALUES (SRC.[rowKey], SRC.[date], SRC.[idStore], SRC.[idProduct], SRC.[unitsSold], SRC.[receipt], SRC.[rowBatchKey]);

        -- Success message and debugging info
        SELECT 'Procedure succeeded' AS [Status], @@ROWCOUNT AS 'RowsAffected';
    END TRY
    BEGIN CATCH
        -- Error handling
        SELECT ERROR_MESSAGE() AS [Error];
    END CATCH;
END
GO


DROP table IF EXISTS [h9].[factSales_qualityLog]
CREATE TABLE [h9].[factSales_qualityLog]
(
    [LogId] INT IDENTITY(1,1) PRIMARY KEY,
    [BatchId] INT,
    [RowKey] NVARCHAR(255),
    [Issue] NVARCHAR(255),
    [LoggedAt] DATETIME2 DEFAULT GETUTCDATE()
);