USE [msdb]
GO
/****** Object:  StoredProcedure [dbo].[Sys_ShrinkLog]    Script Date: 8/6/2018 9:21:25 PM ******/
DROP PROCEDURE IF EXISTS [dbo].[Sys_ShrinkLog]
GO
/****** Object:  StoredProcedure [dbo].[Sys_PathCheck]    Script Date: 8/6/2018 9:21:25 PM ******/
DROP PROCEDURE IF EXISTS [dbo].[Sys_PathCheck]
GO
/****** Object:  StoredProcedure [dbo].[Sys_CreateBackup]    Script Date: 8/6/2018 9:21:25 PM ******/
DROP PROCEDURE IF EXISTS [dbo].[Sys_CreateBackup]
GO
/****** Object:  StoredProcedure [dbo].[Sys_CheckHADR_Databases]    Script Date: 8/6/2018 9:21:25 PM ******/
DROP PROCEDURE IF EXISTS [dbo].[Sys_CheckHADR_Databases]
GO
/****** Object:  StoredProcedure [dbo].[Sys_CheckHADR]    Script Date: 8/6/2018 9:21:25 PM ******/
DROP PROCEDURE IF EXISTS [dbo].[Sys_CheckHADR]
GO
/****** Object:  StoredProcedure [dbo].[Sys_BackupAllDatabases]    Script Date: 8/6/2018 9:21:25 PM ******/
DROP PROCEDURE IF EXISTS [dbo].[Sys_BackupAllDatabases]
GO
/****** Object:  UserDefinedFunction [dbo].[Sys_SplitString]    Script Date: 8/6/2018 9:21:25 PM ******/
DROP FUNCTION IF EXISTS [dbo].[Sys_SplitString]
GO
/****** Object:  UserDefinedFunction [dbo].[Sys_SplitString]    Script Date: 8/6/2018 9:21:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Sys_SplitString]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
BEGIN
execute dbo.sp_executesql @statement = N'
CREATE FUNCTION [dbo].[Sys_SplitString] ( @stringToSplit VARCHAR(MAX) )
RETURNS
 @returnList TABLE ([Name] [nvarchar] (500))
AS
BEGIN

 DECLARE @name NVARCHAR(255)
 DECLARE @pos INT

 WHILE CHARINDEX('','', @stringToSplit) > 0
 BEGIN
  SELECT @pos  = CHARINDEX('','', @stringToSplit)  
  SELECT @name = SUBSTRING(@stringToSplit, 1, @pos-1)

  INSERT INTO @returnList 
  SELECT @name

  SELECT @stringToSplit = SUBSTRING(@stringToSplit, @pos+1, LEN(@stringToSplit)-@pos)
 END

 INSERT INTO @returnList
 SELECT @stringToSplit

 RETURN
END
' 
END
GO
/****** Object:  StoredProcedure [dbo].[Sys_BackupAllDatabases]    Script Date: 8/6/2018 9:21:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Sys_BackupAllDatabases]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[Sys_BackupAllDatabases] AS' 
END
GO

ALTER PROCEDURE [dbo].[Sys_BackupAllDatabases]
(
	@Directory VARCHAR(8000),
	@TypeOfBackup VARCHAR(5),
	@DatabaseList VARCHAR(1000)		= '%',
	@WithCompression BIT			= 1
)
AS 
SET NOCOUNT ON;

DECLARE @DatabaseName SYSNAME;
DECLARE @ERRORVAL VARCHAR(2048)
DECLARE @SQL VARCHAR(2048)
DECLARE @ERRORSUBJECT VARCHAR(2000)

SET @ERRORSUBJECT = 'Database Backup Failed ON ' + @@SERVERNAME

DECLARE Backup_Cursor CURSOR LOCAL STATIC FOR
	SELECT [Name] 
		FROM SYS.Databases 
			WHERE	[State] = 0 AND 
					[name] <> 'tempdb' AND 
					-- Ability to backup one,multiple, or all databases
					([name] in (SELECT [name] 
								FROM dbo.Sys_SplitString (@DatabaseList))
						OR
					  CASE WHEN @DatabaseList = '%' THEN 1 ELSE 0 END = 1 
					 )AND
					 -- Exclude databases with Log shipping 
					 [Name] NOT IN (SELECT primary_database 
									FROM msdb.dbo.log_shipping_primary_databases);

OPEN Backup_Cursor;
FETCH NEXT FROM Backup_Cursor INTO @DatabaseName;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		-- Check if database is HADR and is primary
		DECLARE @Result BIT 
		EXEC dbo.Sys_CheckHADR @DatabaseName = @DatabaseName , @Result = @Result OUTPUT 
		IF @Result = 1
		BEGIN 
			BEGIN TRY 
				-- do backup
				EXEC dbo.Sys_CreateBackup @Directory = @Directory,
									@DatabaseName = @DatabaseName, 
									@TypeOfBackup = @TypeOfBackup,
									@WithCompression = @WithCompression;
				
				IF @TypeOfBackup = 'trn'
				BEGIN
					EXEC dbo.Sys_ShrinkLog @DatabaseName
				END

				IF EXISTS (	SELECT    database_name,MAX(backup_finish_date)
							FROM msdb.dbo.backupset b
							JOIN msdb.dbo.backupmediafamily m ON b.media_set_id = m.media_set_id
							JOIN sys.databases d on d.[name] = b.database_name
							WHERE d.[name] = @DatabaseName
							GROUP BY database_name
							HAVING MAX(backup_finish_date) < DATEADD(day,-8,GETDATE()))
				BEGIN
					SELECT 'No database backup with in 8 days or more';
					SET @ERRORVAL= 'Database: ' + @DatabaseName + ' was not backed-up within 8 days or more';
					SELECT @ERRORVAL;
					SET @ERRORVAL = (SELECT REPLACE(@ERRORVAL,CHAR(10),'<br^>'));
				END
			END TRY 
			BEGIN CATCH
				SET @ERRORVAL= CONVERT(VARCHAR(2048),ISNULL(ERROR_MESSAGE ( ),''));
				SELECT @ERRORVAL;
				SET @ERRORVAL = (SELECT REPLACE(@ERRORVAL,CHAR(10),'<br^>'));

			END CATCH
		END
		FETCH NEXT FROM Backup_Cursor INTO @DatabaseName;
	END;
CLOSE Backup_Cursor;
DEALLOCATE Backup_Cursor;

GO
/****** Object:  StoredProcedure [dbo].[Sys_CheckHADR]    Script Date: 8/6/2018 9:21:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Sys_CheckHADR]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[Sys_CheckHADR] AS' 
END
GO

ALTER PROCEDURE [dbo].[Sys_CheckHADR]
(
	@DatabaseName SYSNAME = '',
	@Result BIT OUTPUT 
)
AS
SET NOCOUNT ON;

DECLARE @TableName SYSNAME;
SET @TableName = 'sys.dm_hadr_database_replica_states';
SET @Result = 1;

-- Check if valid database name was passed
IF EXISTS ( SELECT Name FROM sys.databases WHERE NAME = @DatabaseName)
BEGIN
	-- check if HADR is available on server
	IF CASE WHEN OBJECT_ID(@tablename) IS NOT NULL THEN 1 ELSE 0 END = 1
	BEGIN
			-- check if database is primary
			DECLARE @is_primary_replica SMALLINT
			SELECT @is_primary_replica = SUM(CAST(drs.is_primary_replica as INT))
			FROM sys.dm_hadr_database_replica_states AS drs
				JOIN sys.databases AS db
				ON drs.database_id = db.database_id
				LEFT OUTER JOIN sys.dm_hadr_availability_group_states AS gs 
				ON gs.group_id = drs.group_id
			WHERE Name = @DatabaseName

			IF @is_primary_replica = 1 or @is_primary_replica is NULL
			BEGIN 							
				SET @Result = 1;
			END
			ELSE
			BEGIN
				SET @Result = 0;
			END
	END
END
ELSE
BEGIN
	SET @Result = 0;
END
GO
/****** Object:  StoredProcedure [dbo].[Sys_CheckHADR_Databases]    Script Date: 8/6/2018 9:21:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Sys_CheckHADR_Databases]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[Sys_CheckHADR_Databases] AS' 
END
GO
ALTER PROCEDURE [dbo].[Sys_CheckHADR_Databases]
(
	@DatabaseList VARCHAR(2000),
	@Result BIT OUTPUT
)
AS
SET NOCOUNT ON;

DECLARE @HADR BIT,
		@DBName VARCHAR(256),
		@i INT = 0,
		@Sum INT = 0
DECLARE DBs CURSOR FOR
SELECT [name] FROM [msdb].[dbo].Sys_SplitString (@DatabaseList)
OPEN DBs
FETCH NEXT FROM DBs INTO @DBName
WHILE @@FETCH_STATUS = 0
BEGIN
	EXEC [msdb].[dbo].Sys_CheckHADR @DatabaseName = @DBName , @Result = @HADR OUTPUT
	SET @Sum += CAST(@HADR as INT)
	SET @i += 1
	FETCH NEXT FROM DBs INTO @DBName
END
CLOSE DBs
DEALLOCATE DBs

IF (@Sum/@i) = 1
	SET @Result = 1
ELSE
	SET @Result = 0
GO
/****** Object:  StoredProcedure [dbo].[Sys_CreateBackup]    Script Date: 8/6/2018 9:21:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Sys_CreateBackup]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[Sys_CreateBackup] AS' 
END
GO

ALTER PROCEDURE [dbo].[Sys_CreateBackup]
(
	@Directory VARCHAR(4000),
	@DatabaseName SYSNAME,
	@TypeOfBackup VARCHAR(5),
	@WithCompression BIT =1
)
AS
SET NOCOUNT ON;
BEGIN;
	DECLARE @SQL VARCHAR(8000);
	DECLARE @file_exists INT;
	DECLARE @directory_exists INT;
	DECLARE @parent_directory_exists INT;
	--
	DECLARE @backupfiledate VARCHAR(50) 
	DECLARE @backupfileextention VARCHAR(5) 
	--
	DECLARE @ERRORVAL VARCHAR(2048)
	DECLARE @ERRORSUBJECT VARCHAR(2000)
	SET @ERRORSUBJECT = 'Database Backup Failed ON ' + @@SERVERNAME

END;

-- Sanity check 1
IF @TypeOfBackup NOT IN( 'full','diff','trn') 
BEGIN
	SELECT 'Type of backup should be either: full,diff,or trn';
	RETURN -1;
END

IF @DatabaseName = 'master' AND @TypeOfBackup <> 'full'
BEGIN
	SELECT 'SKIPPING: master database ' + @TypeOfBackup + ' backup';
	RETURN -1;
END

SET @backupfiledate = REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR,GETDATE(),121),'-','_'),' ' , '_'),':',''),'.','_');
SET @backupfileextention = '.' + @TypeOfBackup


-- Sanity 2 
-- Check if database exsists
IF EXISTS( SELECT * FROM Sys.Databases WHERE [Name] = @DatabaseName ) 
	BEGIN;
		-- Check location of the root directory
		EXEC dbo.Sys_PathCheck	@Directory= @Directory ,@file_exists=@file_exists OUTPUT, @Directory_exists	= @directory_exists	OUTPUT,	@parent_directory_exists = @parent_directory_exists	OUTPUT;
		SELECT @file_exists,@directory_exists, @parent_directory_exists; 
		
		-- initial setup check
		IF @directory_exists = 0
		BEGIN
			SELECT 'trying to make root directory'
			EXEC master.dbo.xp_create_subdir @Directory;
			--check again
			EXEC dbo.Sys_PathCheck	@Directory= @Directory ,@file_exists=@file_exists OUTPUT, @Directory_exists	= @directory_exists	OUTPUT,	@parent_directory_exists = @parent_directory_exists	OUTPUT;
			SELECT @file_exists,@directory_exists, @parent_directory_exists; 
		END
		 

	IF @directory_exists = 1 
	BEGIN;
		-- new sub folder should have the name of the database
		SET @Directory = @Directory + '\' + @DatabaseName + '\'
		EXEC dbo.Sys_PathCheck	@Directory= @Directory ,@file_exists=@file_exists OUTPUT, @Directory_exists	= @directory_exists	OUTPUT,	@parent_directory_exists = @parent_directory_exists	OUTPUT;
		IF @directory_exists <> 1 
		BEGIN;
			
			EXEC master.dbo.xp_create_subdir @Directory;
			SELECT 'Direcory created';
		END;

		DECLARE @Ret INT;
		SET @Ret =1;
		CREATE TABLE #Output (ID INT IDENTITY(1,1), OUTPUT VARCHAR(255) NULL)

		IF @TypeOfBackup = 'full' 
		BEGIN;
			SET @SQL = 'BACKUP DATABASE [' + @DatabaseName + '] TO DISK = ''' + @Directory + @DatabaseName + '_backup_' + @backupfiledate + @backupfileextention+ ''' WITH STATS = 1' + CASE WHEN @WithCompression = 1 THEN ',COMPRESSION' ELSE '' END;
			BEGIN TRY 
				SET @SQL =  'sqlcmd -E -S ' + @@SERVERNAME +' -d MSDB -Q "' + @SQL  +'" -b'; 
				INSERT INTO #Output exec @ret = master.dbo.xp_cmdshell @SQL
				IF @ret <> 0
				BEGIN
					SET  @ERRORVAL = (SELECT OUTPUT + ' '  FROM #output FOR XML PATH(''))  
				END

			END TRY 
			BEGIN CATCH
				SELECT 'ERROR: Backing up database ' + @DatabaseName + ' : full';
				SET @ERRORVAL = ( SELECT REPLACE('ERROR: Backing up database ' + @DatabaseName + ' : full.' +ISNULL(ERROR_MESSAGE ( ),''),CHAR(10),'<br^>'));
			END CATCH
		END;
		ELSE IF @TypeOfBackup = 'diff'
		BEGIN;
			SET @SQL = 'BACKUP DATABASE [' + @DatabaseName + '] TO DISK = ''' + @Directory + @DatabaseName + '_backup_' + @backupfiledate + @backupfileextention+ ''' WITH DIFFERENTIAL, STATS = 1' + CASE WHEN @WithCompression = 1 THEN ',COMPRESSION' ELSE '' END;
			BEGIN TRY 
				SET @SQL =  'sqlcmd -E -S ' + @@SERVERNAME +' -d MSDB -Q "' + @SQL  +'" -b'; 
				INSERT INTO #Output exec @ret = master.dbo.xp_cmdshell @SQL
				IF @ret <> 0
				BEGIN
					SET  @ERRORVAL = (SELECT OUTPUT + ' '  FROM #output FOR XML PATH(''))  
				END
			END TRY 
			BEGIN CATCH
				SELECT 'ERROR: Backing up database ' + @DatabaseName + ' : differential';
				SET @ERRORVAL = (SELECT REPLACE('ERROR: Backing up database ' + @DatabaseName + ' : differential.' +ISNULL(ERROR_MESSAGE ( ),''),CHAR(10),'<br^>'));
			
			END CATCH
		END;
		ELSE IF @TypeOfBackup = 'trn'
		BEGIN;
			SET @SQL = 'BACKUP LOG ['	  + @DatabaseName + '] TO DISK = ''' + @Directory + @DatabaseName + '_backup_' + @backupfiledate + @backupfileextention+ ''' WITH STATS = 1' + CASE WHEN @WithCompression = 1 THEN ',COMPRESSION' ELSE '' END;
			IF NOT EXISTS(SELECT * FROM Sys.Databases WHERE [Name] = @DatabaseName AND recovery_model = 3) -- recovery_model = 3 is simple recovery model
			BEGIN;
				BEGIN TRY;
					SET @SQL =  'sqlcmd -E -S ' + @@SERVERNAME +' -d MSDB -Q "' + @SQL  +'" -b'; 
					INSERT INTO #Output exec @ret = master.dbo.xp_cmdshell @SQL
					IF @ret <> 0
					BEGIN
						SET  @ERRORVAL = (SELECT OUTPUT + ' '  FROM #output FOR XML PATH(''))  
					END
				END TRY
				BEGIN CATCH;
				SELECT 'ERROR: Backing up database ' + @DatabaseName + ' : transactional';
				SET @ERRORVAL = (SELECT REPLACE('ERROR: Backing up database ' + @DatabaseName + ' : transactional.' +ISNULL(ERROR_MESSAGE ( ),''),CHAR(10),'<br^>'));
			
				END CATCH;
			END;
			ELSE
			BEGIN;
				SELECT 'SKIPPING: Cant do a transaction backup on a database that is simple recovery mode';
			END;
		END;
		--
		ELSE 
		BEGIN
			SELECT 'ERROR: Unkown database type of backup'; -- well this should not happen
			SET @ERRORVAL = (SELECT REPLACE('ERROR: Unkown database type of backup.',CHAR(10),'<br^>'));
			
		END
	END;
	ELSE
	BEGIN;
		SET @ERRORVAL = (SELECT REPLACE('ERROR: Directory '  + @Directory + ' does not exists.'  ,CHAR(10),'<br^>'));
		SELECT @ERRORVAL 
		
	END;

END;
ELSE
BEGIN;
	SELECT 'ERROR: Database Does not exists';
	SET @ERRORVAL = (SELECT REPLACE('ERROR: Database Does not exists.' ,CHAR(10),'<br^>'));

END;
GO
/****** Object:  StoredProcedure [dbo].[Sys_PathCheck]    Script Date: 8/6/2018 9:21:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Sys_PathCheck]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[Sys_PathCheck] AS' 
END
GO
ALTER PROCEDURE [dbo].[Sys_PathCheck]
(
	@Directory VARCHAR(4000) ,
	@file_exists INT OUTPUT,
	@directory_exists INT OUTPUT,
	@parent_directory_exists INT OUTPUT
	)
AS
SET NOCOUNT ON;

CREATE TABLE #PathCheck ( file_exists BIT, directory_exists BIT, parent_directory_exists BIT );
	
INSERT INTO #PathCheck EXEC master.dbo.xp_fileexist @Directory 
	
SELECT	@file_exists=file_exists,
			@directory_exists = directory_exists, 
			@parent_directory_exists = parent_directory_exists
FROM #PathCheck;

DROP TABLE #PathCheck 
GO
/****** Object:  StoredProcedure [dbo].[Sys_ShrinkLog]    Script Date: 8/6/2018 9:21:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Sys_ShrinkLog]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[Sys_ShrinkLog] AS' 
END
GO
ALTER PROCEDURE [dbo].[Sys_ShrinkLog]
(
	@DatabaseName SYSNAME 
)
AS
BEGIN

	SET NOCOUNT ON;
	DECLARE @SQLcmd NVARCHAR(MAX)
	DECLARE @Shrinkfile nvarchar(256)
	DECLARE @ParmDefinition NVARCHAR(500)

	SET @SQLcmd = N'USE ' + @DatabaseName + '; SELECT @shrinkfileOUT = cast(name as nvarchar(256)) FROM sys.database_files WHERE type = 1'
	SET @ParmDefinition = N'@shrinkfileOUT nvarchar(256) OUTPUT'
	EXECUTE sp_executesql @SQLcmd, @ParmDefinition, @shrinkfileOUT = @shrinkfile OUTPUT

	SET @SQLcmd = N'USE ' + @DatabaseName + ';DBCC SHRINKFILE (' + @shrinkfile + ' , 2000)'

	EXECUTE(@SQLcmd);

END
GO
