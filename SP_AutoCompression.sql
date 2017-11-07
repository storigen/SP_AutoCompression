USE [Database]
GO


CREATE PROC [dbo].[SP_AutoCompression] AS

/***********************************************************************************************************************************
OBJECT NAME:	AutCompression

DESCRIPTION:	Automated Compression, first checks all tables on database, excludes those that are alredy compressed or too small,
				then creates estimates for each table with page/row compression, makes decision to compress and which method, and
				finally compresses all tables using page/row compression.
				
				If the savings is at least 10% of space, then will compress. If page is 10% greater than row, then uses page.
***********************************************************************************************************************************/





--------------------------------------------------------------------------------------------------------------
-- 1 CREATE TEMP TABLES/VARIABLES TO HOLD COMPRESSION ESTIMATES
--------------------------------------------------------------------------------------------------------------
SET NOCOUNT ON;


DECLARE @tableName	VARCHAR(256)
  , @schemaName	  VARCHAR(100)
  , @sqlStatement	VARCHAR(1000)
  , @statusMsg	  VARCHAR(1000)
  , @U_tableName	VARCHAR(256)
  , @U_schemaName	VARCHAR(100)	
  , @compression	VARCHAR (4)
  , @sql_exec		  VarChar(1000);


IF object_id('tempdb..#u_tables')IS NOT NULL BEGIN DROP TABLE #u_tables END

if object_id('tempdb..#tables')	is not null begin drop table #tables end
	CREATE TABLE #tables
		( schemaName        sysname NULL
		, tableName         sysname NULL
		, page_processed    bit
		, row_processed		  bit);
 

if object_id('tempdb..#updates')	is not null begin drop table #updates end
	CREATE TABLE #updates
		( tableName        sysname NULL
		, spName           sysname NULL)



if object_id('tempdb..#row_compression')	is not null begin drop table #row_compression end
--IF EXISTS(SELECT * FROM tempdb.sys.tables WHERE name LIKE '%#row_compression%')  DROP TABLE #row_compression;
     CREATE TABLE #row_compression
		( objectName                    varchar(100)
		, schemaName                    varchar(50)
		, index_id                      int
		, partition_number              int
		, size_current_compression      bigint
		, size_requested_compression    bigint
		, sample_current_compression    bigint
		, sample_requested_compression  bigint);


if object_id('tempdb..#page_compression')	is not null begin drop table #page_compression end
     CREATE TABLE #page_compression
		( objectName                    varchar(100)
		, schemaName                    varchar(50)
		, index_id                      int
		, partition_number              int
		, size_current_compression      bigint
		, size_requested_compression    bigint
		, sample_current_compression    bigint
		, sample_requested_compression  bigint);

 



--------------------------------------------------------------------------------------------------------------
-- 2 GENERATE TABLE LIST FOR COMPRESSION ESTIMATES: Exclude (2.1) compressed, (2.2) small, (2.3) 
--------------------------------------------------------------------------------------------------------------

PRINT '-------------------------------------------------------------------------------------'
PRINT 'PREP - Generating List of Tables and Exclusions'
PRINT '-------------------------------------------------------------------------------------'
PRINT ' '


INSERT INTO #tables

SELECT DISTINCT s.name AS schemaName, t.name AS tableName, 0, 0
FROM sys.tables T
JOIN sys.schemas s ON t.schema_id = s.schema_id

LEFT JOIN
	(	
		
		-- 2.1 Exclude compressed tables
		----------------------------------------------------------------------------------------------------------
		SELECT DISTINCT SCHEMA_NAME(o.schema_id) as SchemaName,
			OBJECT_NAME(o.object_id) as TableName,p.data_compression_desc AS CompressionType
		FROM sys.partitions  p 
			INNER JOIN sys.objects o ON p.object_id = o.object_id 
		WHERE p.data_compression > 0 AND SCHEMA_NAME(o.schema_id) <> 'SYS' 
		-------------------------------------------------------------------------------------
	) Cx ON Cx.SchemaName=s.name AND Cx.TableName=t.Name





LEFT JOIN 
	(	
		
		-- 2.2 Exclude small tables
		----------------------------------------------------------------------------------------------------------
		SELECT * 
		FROM	(
					SELECT s.Name AS SchemaName, t.NAME AS TableName, SUM(a.used_pages)*8  AS UsedSpaceKB
					FROM   sys.tables t
						INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
						INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
						INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
						LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
					GROUP BY t.Name, s.Name
				) X 
		WHERE UsedSpaceKB < '99999'
		-------------------------------------------------------------------------------------
	) Sm ON Sm.SchemaName=s.Name AND Sm.TableName=t.Name

	WHERE CX.TableName IS NULL AND SM.TableName IS NULL

	
	SELECT * INTO #U_TABLES FROM #TABLES






	-- 2.3 OPTION TO EXCLUDE PRODUCTION TABLES (Exclude objects that have frequent sproc writes, only include static tables) 
	--------------------------------------------------------------------------------------------------------------
	/*
	WHILE EXISTS(select *  from #U_TABLES)
	BEGIN


				--A. Select a table to examine
				----------------------------------------------------------------------------------------------------------
				SELECT TOP 1 @u_tableName = tableName, @u_schemaName = schemaName
				FROM #U_TABLES 

	
				--B. Examine if table has writes but no drop/recreates
				----------------------------------------------------------------------------------------------------------
				INSERT INTO #UPDATES 
				SELECT DISTINCT @u_tablename, object_name(id)
				FROM  SYS.syscomments 
				WHERE 
					(	TEXT LIKE ('%INTO ' + 'dbo.' + @u_tablename + '%')
				 OR TEXT LIKE ('%DELETE FROM ' + 'dbo.' + @u_tablename + '%')
				 OR TEXT LIKE ('%UPDATE ' + 'dbo.' + @u_tablename + '%')
					)
				AND @U_tableName NOT IN		(
												SELECT DISTINCT @u_tablename AS u_tablename
												FROM  SYS.syscomments 
												WHERE TEXT LIKE ('%DROP TABLE ' + 'dbo.' + @u_tablename + '%')									

											)

				--C. Update selection choices, remove table that was scanned in B
				----------------------------------------------------------------------------------------------------------
				DELETE FROM #U_TABLES WHERE @u_tableName = tableName AND @u_schemaName = schemaName
	END


	--2.4 Delete write-tables identified in loop from #tables
	----------------------------------------------------------------------------------------------------------
	DELETE FROM #TABLES 
	WHERE tablename in (SELECT DISTINCT tableName from #UPDATES)
	*/













--------------------------------------------------------------------------------------------------------------
-- 3 ESTIMATE COMPRESSION FOR ALL TABLES FROM STEP 2
--------------------------------------------------------------------------------------------------------------

PRINT '-------------------------------------------------------------------------------------'
PRINT 'ESTIMATE '
PRINT '-------------------------------------------------------------------------------------'


	-- 3.1 Process Row Compression
	----------------------------------------------------------------------------------------------------------
	WHILE EXISTS(SELECT * FROM #tables WHERE row_processed = 0)
	BEGIN
 
		--A. Pick first unproccessed table
		----------------------------------------------------------------------------------------------------------
		SELECT TOP 1 @tableName = tableName, @schemaName = schemaName
		FROM #tables WHERE row_processed = 0;


		--B. Create sp estimate string
		----------------------------------------------------------------------------------------------------------
		SET @sqlStatement = 'EXECUTE sp_estimate_data_compression_savings ''' +@schemaName+ ''', ''' +@tableName+ ''', NULL, NULL, ''ROW'';'
 

		--C. Execute sp estimate string
		----------------------------------------------------------------------------------------------------------
		BEGIN
			PRINT 'Row Estimate for ' + @tableName;
			INSERT INTO #row_compression
			EXECUTE sp_executesql @sqlStatement;
		END;

		--E. Update table as processed 
		---------------------------------------------------------------------------------------------------------- 
		UPDATE #tables
		SET row_processed = 1
		WHERE tableName = @tableName AND schemaName = @schemaName;
 
	END;
 



	-- 3.2 Process Row Compression
	----------------------------------------------------------------------------------------------------------
	WHILE EXISTS(SELECT * FROM #tables WHERE page_processed = 0)
	BEGIN
 
		--A. Pick first unproccessed table
		----------------------------------------------------------------------------------------------------------
		SELECT TOP 1 @tableName = tableName, @schemaName = schemaName
		FROM #tables WHERE page_processed = 0;


		--C. Create sp estimate string
		----------------------------------------------------------------------------------------------------------
		SET @sqlStatement = 'EXECUTE sp_estimate_data_compression_savings ''' +@schemaName+ ''', ''' +@tableName+ ''', NULL, NULL, ''PAGE'';'
 

		--D. Execute sp estimate string
		----------------------------------------------------------------------------------------------------------
		BEGIN
			PRINT 'Page Estimtate for ' + @tableName;
			INSERT INTO #page_compression
			EXECUTE sp_executesql @sqlStatement;
		END;

		--E. Update table as processed 
		---------------------------------------------------------------------------------------------------------- 
		UPDATE #tables
		SET page_processed = 1
		WHERE tableName = @tableName AND schemaName = @schemaName;
 
	END;








--------------------------------------------------------------------------------------------------------------
-- 4. List estimated savings, make recommendations 
--------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#compression_estimates') IS NOT NULL DROP TABLE #compression_estimates
IF OBJECT_ID('tempdb..#ctables') IS NOT NULL DROP TABLE #ctables



	-- 4.1 Compile row and page estimates
	----------------------------------------------------------------------------------------------------------
	SELECT pg.schemaName, pg.objectName as TableName, pg.size_current_compression as original_size, pg.page_size, pg.page_compression, rw.row_size, rw.row_compression
	INTO #compression_estimates

	FROM

		(
			SELECT schemaname, objectname, 'page' as kind, size_current_compression, size_requested_compression as page_size,
				CASE WHEN size_current_compression = 0 THEN 0 
				WHEN  size_requested_compression = 0 THEN 0
				ELSE 1-CAST(size_requested_compression AS FLOAT)/size_current_compression
				END AS page_compression
			FROM #page_compression
			WHERE index_id in (0,1)
	
		) as pg

	JOIN

		(
			SELECT schemaname, objectname, 'row' as kind, size_current_compression, size_requested_compression as row_size,
				CASE WHEN size_current_compression = 0 THEN 0 
				WHEN  size_requested_compression = 0 THEN 0
				ELSE 1-CAST(size_requested_compression AS FLOAT)/size_current_compression
				END AS row_compression
			FROM #row_compression
			WHERE index_id in (0,1)

		) as rw

	ON rw.schemaName=pg.schemaName AND rw.objectName=pg.objectName


	-- 4.2 Determine whether row or page is ideal
	----------------------------------------------------------------------------------------------------------
	SELECT schemaName, TableName, original_size, page_size, row_size, page_compression, row_compression 
	,'Decision' = CASE WHEN page_compression > .299 AND (PAGE_compression-row_compression) > .1 THEN 'page'
				  WHEN row_compression > .19 AND (PAGE_compression-row_compression) <.2 THEN 'row' 
				  ELSE 'na' END
	,0 as compressed
	INTO #ctables
	FROM  #compression_estimates
	ORDER BY original_size
	--drop table #compression_estimates
	--drop table #ctables











--------------------------------------------------------------------------------------------------------------
-- 5. Compress tables based on algorithm  in 4.2 
--------------------------------------------------------------------------------------------------------------


PRINT '-------------------------------------------------------------------------------------'
PRINT 'COMPRESSING '
PRINT '-------------------------------------------------------------------------------------'


WHILE EXISTS (SELECT * FROM #CTABLES WHERE compressed = 0 AND decision IN ('ROW','PAGE')) 
BEGIN


	-- 5.1 Declare Table to be compressed
	----------------------------------------------------------------------------------------------------------
	SELECT TOP 1 @tableName = tableName, @schemaName = schemaName, @compression = decision
	FROM #ctables WHERE compressed = 0 AND decision IN ('ROW','PAGE') 
	--select @tableName, @schemaName, @compression



	-- 5.2 Compress as row or page
	----------------------------------------------------------------------------------------------------------
	IF @compression = 'row'

		BEGIN
			PRINT 'Row Compression on ' + @tableName;
			SELECT @sql_exec = 'ALTER TABLE ' + @tableName +' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = ROW)';
			Exec (@sql_exec)			
			--print @sql_exec
		END

	ELSE 

		BEGIN
			PRINT 'Page Compression on ' + @tableName;
			SELECT @sql_exec = 'ALTER TABLE ' + @tableName +' REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)';
			Exec (@sql_exec)			
			--print @sql_exec
		END




	-- 5.3 Mark processed table for loop
	----------------------------------------------------------------------------------------------------------
	UPDATE #ctables 
	SET compressed = 1 
	WHERE @tableName = tableName AND @schemaName = schemaName AND @compression = decision


END


SELECT * FROM #ctables where compressed = 1
