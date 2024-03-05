DROP TABLE if exists [h9].[dimCalendar]
go
CREATE TABLE [h9].[dimCalendar]
(
[datekey] [int] not null,
[date] [date], 
[year] [smallint], 
[monthNo] [smallint],
[monthName] [nvarchar](10),
[yyyy-mm] [nvarchar](7),
[week] [smallint],
[yyyy-ww] [nvarchar](7),
-- CTRL
[rowBatchKey] [int] not null,
[rowCreated] [datetime] not null default getutcdate()
, [rowModified] [datetime] not null default getutcdate()
CONSTRAINT [pk_dimCalendar] PRIMARY KEY CLUSTERED ([datekey]),
);
go


DROP TABLE if exists [H9].[dimCalendar_stg]
go
CREATE TABLE [H9].[dimCalendar_stg]
(
[datekey] [int] not null,
[date] [date], 
[year] [smallint], 
[monthNo] [smallint],
[monthName] [nvarchar](10),
[yyyy-mm] [nvarchar](7),
[week] [smallint],
[yyyy-ww] [nvarchar](7),
-- CTRL
[rowBatchKey] [int] not null,
[rowCreated] [datetime] not null default getutcdate(),
CONSTRAINT [pk_dimCalendar_stg] PRIMARY KEY CLUSTERED ([datekey]),
);
go


drop procedure if exists [h9].[dimCalendar_postprocess];
go
create procedure [h9].[dimCalendar_postprocess]
@BatchId int
as
    TRUNCATE table [H9].[dimCalendar_stg];
    SELECT dummyval = 2
    RETURN 1
go


DROP PROCEDURE IF EXISTS [h9].[dimCalendar_publish];
GO
CREATE PROCEDURE [h9].[dimCalendar_publish]
@BatchId INT
AS
BEGIN
    SET NOCOUNT ON; -- Suppress "X rows affected" message

    BEGIN TRY
        -- Merge data from staging table into target table
        MERGE INTO [h9].[dimCalendar] AS TRG
        USING (
            SELECT 
              [datekey],
              [date], 
              [year], 
              [monthNo],
              [monthName],
              [yyyy-mm],
              [week],
              [yyyy-ww],
              [rowBatchKey]
            FROM 
                [h9].[dimCalendar_stg]
            WHERE 
                [rowBatchKey] = @BatchId
        ) AS SRC
        ON TRG.[datekey] = SRC.[datekey] -- Assuming datekey is the unique identifier
        WHEN MATCHED THEN
            UPDATE SET 
                TRG.[date] = SRC.[date],
                TRG.[year] = SRC.[year],
                TRG.[monthNo] = SRC.[monthNo],
                TRG.[monthName] = SRC.[monthName],
                TRG.[yyyy-mm] = SRC.[yyyy-mm],
                TRG.[week] = SRC.[week],
                TRG.[yyyy-ww] = SRC.[yyyy-ww],
                TRG.[rowModified] = GETUTCDATE(),
                TRG.[rowBatchKey] = SRC.[rowBatchKey] -- Update rowBatchKey if necessary
        WHEN NOT MATCHED THEN
            INSERT ([datekey], [date], [year], [monthNo], [monthName], [yyyy-mm], [week], [yyyy-ww], [rowBatchKey], [rowCreated], [rowModified])
            VALUES (SRC.[datekey], SRC.[date], SRC.[year], SRC.[monthNo], SRC.[monthName], SRC.[yyyy-mm], SRC.[week], SRC.[yyyy-ww], SRC.[rowBatchKey], GETUTCDATE(), GETUTCDATE());

        -- Success message
        SELECT 'Procedure succeeded' AS [Status];
    END TRY
    BEGIN CATCH
        -- Error message
        SELECT ERROR_MESSAGE() AS [Error];
    END CATCH;
END
GO

select * from h9.dimCalendar