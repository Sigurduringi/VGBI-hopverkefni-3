DROP TABLE if exists [H9].[dimProduct_stg]
go
CREATE TABLE [H9].[dimProduct_stg]
(
[id] [int] identity(1,1) not null
, [rowKey] [nvarchar](200) not null
, [name] [nvarchar](50)
, [category] [nvarchar](50)
, [cost] [decimal](19,2)
, [price] [decimal](19,2)
-- CTRL
, [rowBatchKey] [int] not null
, [rowCreated] [datetime] not null default getutcdate()
, CONSTRAINT [pk_dimProduct_stg] PRIMARY KEY CLUSTERED ([id])
);
go
-- sama og að setja unique key á [rowKey] því við ákveðum gildið í rowBatchKey
create unique index UIX_dimIProduct_stg_BatchId_rowKey on [H9].[dimProduct_stg]
([rowBatchKey],[rowKey]);
go

-- PROCEDURES 

DROP PROCEDURE IF EXISTS [h9].[dimProduct_publish];
GO
CREATE PROCEDURE [h9].[dimProduct_publish]
@BatchId INT
AS
BEGIN
    SET NOCOUNT ON; -- Suppress  "X rows affected" message

    BEGIN TRY
        -- Merge-a gögnum úr staging töflu og yfir í target töfluna
        MERGE INTO [h9].[dimProduct] AS TRG
        USING (
            SELECT 
                [rowKey],
                [name],
                [category] = ISNULL([category], 'n/a'),
                [cost] = ISNULL([cost], 0),
                [price] = ISNULL([price], 0),
                [rowBatchKey]
            FROM 
                [h9].[dimProduct_stg]
            WHERE 
                [rowBatchKey] = @BatchId
        ) AS SRC
        ON SRC.rowKey = TRG.rowKey
        AND SRC.rowBatchKey = @BatchId
        WHEN MATCHED THEN
            UPDATE SET 
                TRG.[name] = SRC.[name],
                TRG.[category] = SRC.[category],
                TRG.[cost] = SRC.[cost],
                TRG.[price] = SRC.[price],
                TRG.[rowModified] = GETUTCDATE()
        WHEN NOT MATCHED THEN
            INSERT ([rowKey], [name], [category], [cost], [price], [rowBatchKey])
            VALUES (SRC.[rowKey], SRC.[name], SRC.[category], SRC.[cost], SRC.[price], SRC.[rowBatchKey]);

        --  success message
        SELECT 'Procedure executed successfully' AS [Status];
    END TRY
    BEGIN CATCH
        --  error message
        SELECT ERROR_MESSAGE() AS [Error];
    END CATCH;
END
GO