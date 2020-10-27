USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[Sys_RestoreProdtoDev]
(
	@sourcedbname VARCHAR(255),
	@targetdbname VARCHAR(255),
	@pathname VARCHAR(255),
	@movetopath VARCHAR(255) = NULL,
	@FullOnly BIT = 0
)
AS
SET NOCOUNT ON;

CREATE TABLE #FinalOutput (FileDate DATETIME, FilePath VARCHAR(8000), OUTPUT VARCHAR(8000) NULL)

DECLARE @ret INT = 0;
DECLARE @CMD VARCHAR(8000);
SET @CMD =  'dir /O-d '+@pathname+'\*';
CREATE TABLE #Output (ID INT IDENTITY(1,1), [OUTPUT] VARCHAR(8000) NULL)
INSERT INTO #Output EXEC @ret = master.dbo.xp_cmdshell @CMD
INSERT INTO #FinalOutput
SELECT TRY_CONVERT(DATETIME,LEFT([OUTPUT],20)) AS FileDate,
	@pathname AS FilePath,
	[OUTPUT]
FROM #Output
WHERE [OUTPUT] LIKE '%' + @sourcedbname + '%'
	AND ([OUTPUT] LIKE '%.FULL%' OR [OUTPUT] LIKE '%.BAK%' OR [OUTPUT] LIKE '%.DIFF%')

select * from #FinalOutput

DROP TABLE #Output

-- STEP 1: Set database single user mode in order to avoid blocking
SELECT [name]
FROM sys.databases
WHERE state_desc='online'
AND [name] = @targetdbname
if @@ROWCOUNT>0
BEGIN

	DECLARE @dynamic_statement_alterDB varchar (8000)
	SET @dynamic_statement_alterDB = 'ALTER DATABASE ['+@targetdbname+'] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;'
	SELECT @dynamic_statement_alterDB
	EXEC (@dynamic_statement_alterDB)

END
-- STEP 2: Restore FULL
DECLARE @destination VARCHAR(8000);
DECLARE @dest_path VARCHAR(8000);

SELECT TOP 1 @dest_path = FilePath,
	@destination = SUBSTRING([OUTPUT] ,40, 1000)
FROM #FinalOutput
WHERE [OUTPUT] LIKE '%' + @sourcedbname + '%'
	AND ([OUTPUT] LIKE '%.FULL%' OR [OUTPUT] LIKE '%.BAK%')
ORDER BY FileDate DESC


	DECLARE @fileListTable TABLE
	(
		LogicalName          nvarchar(128),
		PhysicalName         nvarchar(260),
		[Type]               char(1),
		FileGroupName        nvarchar(128),
		Size                 numeric(20,0),
		MaxSize              numeric(20,0),
		FileID               bigint,
		CreateLSN            numeric(25,0),
		DropLSN              numeric(25,0),
		UniqueID             uniqueidentifier,
		ReadOnlyLSN          numeric(25,0),
		ReadWriteLSN         numeric(25,0),
		BackupSizeInBytes    bigint,
		SourceBlockSize      int,
		FileGroupID          int,
		LogGroupGUID         uniqueidentifier,
		DifferentialBaseLSN  numeric(25,0),
		DifferentialBaseGUID uniqueidentifier,
		IsReadOnl            bit,
		IsPresent            bit,
		TDEThumbprint        varbinary(32),
		SnapshotUrl          nvarchar(360)
	)
	SET @CMD = 'restore filelistonly from disk = ''' + @dest_path + '\' + @destination +''''
	INSERT INTO @fileListTable EXEC (@CMD);

	select * from @fileListTable

SET @CMD = 'RESTORE DATABASE ['+@targetdbname+'] FROM  DISK = ''' + @dest_path + '\' + @destination +''' WITH FILE = 1,  NORECOVERY,  NOUNLOAD,  REPLACE,  STATS = 1 '

DECLARE @LogicalName nvarchar(128),
		@Type CHAR(1),
		@PhysicalName nvarchar(260)

DECLARE ProdToDEV_Cursor CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
SELECT LogicalName,
	[Type],
	PhysicalName
FROM @fileListTable
OPEN ProdToDEV_Cursor ;
FETCH NEXT FROM ProdToDEV_Cursor INTO @LogicalName, @Type, @PhysicalName;
WHILE @@FETCH_STATUS = 0
   BEGIN

		DECLARE @Extention VARCHAR(10) = CASE @Type WHEN 'L' THEN '_LOG.ldf' ELSE '.mdf' END;

		IF @movetopath is not NULL
		SET @CMD = @CMD + ', MOVE '''+ @LogicalName + ''' TO ''' + @movetopath + '\' + @targetdbname + @Extention + ''''
		ELSE
		SET @CMD = @CMD + ', MOVE '''+ @LogicalName + ''' TO ''' + @PhysicalName + ''''

		FETCH NEXT FROM ProdToDEV_Cursor INTO @LogicalName, @Type, @PhysicalName;

   END;
CLOSE ProdToDEV_Cursor;
DEALLOCATE ProdToDEV_Cursor;

SELECT @CMD
EXEC (@CMD)

IF @FullOnly = 1  GOTO Recovery_Steps;

-- STEP 3: Restore DIFFS
DECLARE Diff_outer_Cursor CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
SELECT TOP 1 FilePath,
	SUBSTRING([OUTPUT] ,40, 1000) Destination
FROM #FinalOutput D
JOIN (
		SELECT TOP 1 FileDate
		FROM #FinalOutput
		WHERE [OUTPUT] LIKE '%' + @sourcedbname + '%'
			AND ([OUTPUT] LIKE '%.FULL%' OR [OUTPUT] LIKE '%.BAK%')
		ORDER BY FileDate DESC
	) F
ON F.FileDate < D.FileDate
WHERE D.[OUTPUT] LIKE '%' + @sourcedbname + '%'
	AND D.[OUTPUT] LIKE '%.DIFF%'
ORDER BY D.FileDate DESC

	OPEN Diff_outer_Cursor;
	FETCH NEXT FROM Diff_outer_Cursor INTO @dest_path, @destination;
	WHILE @@FETCH_STATUS = 0
	   BEGIN

			DECLARE @fileListTable_diff TABLE
			(
				LogicalName          nvarchar(128),
				PhysicalName         nvarchar(260),
				[Type]               char(1),
				FileGroupName        nvarchar(128),
				Size                 numeric(20,0),
				MaxSize              numeric(20,0),
				FileID               bigint,
				CreateLSN            numeric(25,0),
				DropLSN              numeric(25,0),
				UniqueID             uniqueidentifier,
				ReadOnlyLSN          numeric(25,0),
				ReadWriteLSN         numeric(25,0),
				BackupSizeInBytes    bigint,
				SourceBlockSize      int,
				FileGroupID          int,
				LogGroupGUID         uniqueidentifier,
				DifferentialBaseLSN  numeric(25,0),
				DifferentialBaseGUID uniqueidentifier,
				IsReadOnl            bit,
				IsPresent            bit,
				TDEThumbprint        varbinary(32),
				SnapshotUrl          nvarchar(360)
			)
			SET @CMD = 'restore filelistonly from disk = '''+@dest_path + '\' + @destination+''''
			INSERT INTO @fileListTable_diff EXEC (@CMD);

			select * from @fileListTable_diff

			SET @CMD = 'RESTORE DATABASE ['+@targetdbname+'] FROM  DISK = '''+@dest_path + '\' + @destination+''' WITH FILE = 1,  NORECOVERY,  NOUNLOAD,  REPLACE,  STATS = 1 '

			DECLARE ProdToDEV_Cursor CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
			SELECT LogicalName,
				[Type],
				PhysicalName
			FROM @fileListTable_diff
			OPEN ProdToDEV_Cursor ;

			FETCH NEXT FROM ProdToDEV_Cursor INTO @LogicalName, @Type, @PhysicalName;
			WHILE @@FETCH_STATUS = 0
			   BEGIN

					DECLARE @Extention_diff VARCHAR(10)= CASE @Type WHEN 'L' THEN '_LOG.ldf' ELSE '.mdf' END;

					IF @movetopath is not NULL
					SET @CMD = @CMD + ', MOVE '''+ @LogicalName + ''' TO ''' + @movetopath + '\' + @targetdbname + @Extention_diff + ''''
					ELSE
					SET @CMD = @CMD + ', MOVE '''+ @LogicalName + ''' TO ''' + @PhysicalName + ''''

					FETCH NEXT FROM ProdToDEV_Cursor INTO @LogicalName, @Type, @PhysicalName;

			   END;
			CLOSE ProdToDEV_Cursor;
			DEALLOCATE ProdToDEV_Cursor;

			SELECT @CMD;
			EXEC (@CMD)

		  FETCH NEXT FROM Diff_outer_Cursor INTO @dest_path, @destination;

	   END;
	CLOSE Diff_outer_Cursor;
	DEALLOCATE Diff_outer_Cursor;

Recovery_Steps:
SET @dynamic_statement_alterDB = 'RESTORE DATABASE ['+@targetdbname+'] WITH RECOVERY'
SELECT @dynamic_statement_alterDB
EXEC (@dynamic_statement_alterDB)

SET @dynamic_statement_alterDB = 'ALTER DATABASE ['+@targetdbname+'] SET RECOVERY SIMPLE WITH NO_WAIT'
SELECT @dynamic_statement_alterDB
EXEC (@dynamic_statement_alterDB)

SET @dynamic_statement_alterDB = 'ALTER DATABASE ['+@targetdbname+'] SET MULTI_USER'
SELECT @dynamic_statement_alterDB
EXEC (@dynamic_statement_alterDB)
