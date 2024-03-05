DROP TABLE if exists [h9].[dimStores]
go
CREATE TABLE [h9].[dimStores]
(
[id] [int] identity(1,1) not null
, [rowKey] [nvarchar](200)
, [name] [nvarchar](50) not NULL
, [city] [nvarchar](50) not null
, [location] [nvarchar](50) not null
-- CTRL
, [rowBatchKey] [int] not null
, [rowCreated] [datetime] not null default getutcdate()
, [rowModified] [datetime] not null default getutcdate() -- <<-- ef incremental
, CONSTRAINT [pk_dimStores] PRIMARY KEY CLUSTERED ([id])
);
go
-- ATH: rowBatchKey ekki lengur hluti af UIX
create unique index UIX_dimStores_rowKey on [h9].[dimStores] ([rowKey]);
go



drop procedure if exists [h9].[dimSales_publish];
go
create procedure [h9].[dimSales_publish]
@BatchId int
as
--
-- hér er hægt að framkvæma gæða tjékk og ákveða með framhaldið
--
Merge into [h9].[dimSales] TRG
using [h9].[dimSales_stg] SRC
on SRC.rowKey = TRG.rowKey
and src.rowBatchKey = @BatchId
When Matched then
Update Set [date] = src.[date]
, [idStore] = src.[idStore]
, [idProduct] = src.[idProduct]
, [unitsSold] = src.[unitsSold]
, [receipt] = src.[receipt]
, [rowBatchKey] = src.[rowBatchKey] -- eða @BatchId
, [rowModified] = getutcdate()
When Not Matched then
insert
( [rowKey]
, [date]
, [idStore]
, [idProduct]
, [unitsSold]
, [receipt]
, [rowBatchKey]
)
values
( src.[rowKey]
, src.[date]
, src.[idStore]
, src.[idProduct]
, src.[unitsSold]
, src.[receipt]
, src.[rowBatchKey] -- eða @BatchId
)
34c74591-1f6a-41e7-81ef-528459edc603; -- MERGE verður að enda á semikommu
go


DROP TABLE if exists [H9].[dimStores_stg]
go
CREATE TABLE [H9].[dimStores_stg]
(
[id] [int] identity(1,1) not null
, [rowKey] [nvarchar](200)

, [name] [nvarchar](50) not NULL
, [city] [nvarchar](50) not null
, [location] [nvarchar](50) not null
-- CTRL
, [rowBatchKey] [int] not null
, [rowCreated] [datetime] not null default getutcdate()
, CONSTRAINT [pk_dimStores_stg] PRIMARY KEY CLUSTERED ([id])
);
go
-- sama og að setja unique key á [rowKey] því við ákveðum gildið í rowBatchKey
create unique index UIX_dimStores_stg_BatchId_rowKey on [H9].[dimStores_stg]
([rowBatchKey],[rowKey]);
go



drop procedure if exists [h9].[dimStores_postprocess];
go
create procedure [h9].[dimStores_postprocess]
@BatchId int
as
    TRUNCATE table [H9].[dimStores_stg];
    SELECT dummyval = 2
    RETURN 1
go