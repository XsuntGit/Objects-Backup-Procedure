USE [msdb]
GO
DROP PROCEDURE IF EXISTS [dbo].[Sys_ShrinkLog]
GO
DROP PROCEDURE IF EXISTS [dbo].[Sys_PathCheck]
GO
DROP PROCEDURE IF EXISTS [dbo].[Sys_CreateBackup]
GO
DROP PROCEDURE IF EXISTS [dbo].[Sys_CheckHADR_Databases]
GO
DROP PROCEDURE IF EXISTS [dbo].[Sys_CheckHADR]
GO
DROP PROCEDURE IF EXISTS [dbo].[Sys_BackupAllDatabases]
GO
DROP FUNCTION IF EXISTS [dbo].[Sys_SplitString]
GO


USE [XsuntAdmin]
GO


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [dbo].[Sys_PathCheck]
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


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [dbo].[Sys_ShrinkLog]
(
	@DatabaseName SYSNAME
)
AS
SET NOCOUNT ON;
BEGIN

	DECLARE @SQLcmd NVARCHAR(MAX)
	DECLARE @Shrinkfile nvarchar(256)
	DECLARE @ParmDefinition NVARCHAR(500)

	SET @SQLcmd = N'USE ' + @DatabaseName + '; SELECT @shrinkfileOUT = cast(name as nvarchar(256)) FROM sys.database_files WHERE type = 1'
	SET @ParmDefinition = N'@shrinkfileOUT nvarchar(256) OUTPUT'
	EXECUTE sp_executesql @SQLcmd, @ParmDefinition, @shrinkfileOUT = @shrinkfile OUTPUT

	SET @SQLcmd = N'USE ' + @DatabaseName + '; DBCC SHRINKFILE (' + @shrinkfile + ' , 2000)'

	EXECUTE(@SQLcmd);

END
GO


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[Sys_CheckHADR]
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
		DECLARE @is_primary_replica SMALLINT,
				@synchronization_state SMALLINT
		SELECT @is_primary_replica = SUM(CAST(drs.is_primary_replica as INT)),
			@synchronization_state = SUM(CAST(drs.synchronization_state as INT))
		FROM sys.dm_hadr_database_replica_states AS drs
			JOIN sys.databases AS db
			ON drs.database_id = db.database_id
			LEFT OUTER JOIN sys.dm_hadr_availability_group_states AS gs
			ON gs.group_id = drs.group_id
		WHERE Name = @DatabaseName

		IF @synchronization_state = 1
		BEGIN
			SET @Result = NULL;
		END
		ELSE
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

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER PROCEDURE [dbo].[Sys_CreateBackup]
(
	@Directory VARCHAR(4000),
	@DatabaseName SYSNAME,
	@TypeOfBackup VARCHAR(5),
	@WithCompression BIT = 1,
	@MaxTransferSize INT,
	@is_encrypted BIT,
	@encryption_state INT,
	@CopyOnly BIT
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
		SELECT @file_exists, @directory_exists, @parent_directory_exists;

		-- initial setup check
		IF @directory_exists = 0
		BEGIN
			SELECT 'trying to make root directory'
			EXEC master.dbo.xp_create_subdir @Directory;
			--check again
			EXEC dbo.Sys_PathCheck	@Directory= @Directory ,@file_exists=@file_exists OUTPUT, @Directory_exists	= @directory_exists	OUTPUT,	@parent_directory_exists = @parent_directory_exists	OUTPUT;
			SELECT @file_exists, @directory_exists, @parent_directory_exists;
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
			SET @SQL = 'USE [master]; BACKUP DATABASE [' + @DatabaseName + '] TO DISK = ''' + @Directory + @DatabaseName + '_backup_' + @backupfiledate + @backupfileextention+ ''' WITH STATS = 1' + CASE WHEN @WithCompression = 1 THEN ', COMPRESSION' ELSE '' END + CASE WHEN @MaxTransferSize > 0 and @is_encrypted = 1 and @encryption_state = 3 THEN ', MAXTRANSFERSIZE = ' + CAST(@MaxTransferSize as VARCHAR(20)) ELSE '' END + CASE WHEN @CopyOnly = 1 THEN ', COPY_ONLY' ELSE '' END;
			BEGIN TRY
				EXECUTE(@SQL)
			END TRY
			BEGIN CATCH
				SELECT 'ERROR: Backing up database ' + @DatabaseName + ' : full';
				SET @ERRORVAL = ( SELECT REPLACE('ERROR: Backing up database ' + @DatabaseName + ' : full.' +ISNULL(ERROR_MESSAGE ( ),''),CHAR(10),'<br^>'));
			END CATCH
		END;
		ELSE IF @TypeOfBackup = 'diff'
		BEGIN;
			SET @SQL = 'USE [master]; BACKUP DATABASE [' + @DatabaseName + '] TO DISK = ''' + @Directory + @DatabaseName + '_backup_' + @backupfiledate + @backupfileextention+ ''' WITH DIFFERENTIAL, STATS = 1' + CASE WHEN @WithCompression = 1 THEN ', COMPRESSION' ELSE '' END + CASE WHEN @MaxTransferSize > 0 and @is_encrypted = 1 and @encryption_state = 3 THEN ', MAXTRANSFERSIZE = ' + CAST(@MaxTransferSize as VARCHAR(20)) ELSE '' END;
			BEGIN TRY
				EXECUTE(@SQL)
			END TRY
			BEGIN CATCH
				SELECT 'ERROR: Backing up database ' + @DatabaseName + ' : differential';
				SET @ERRORVAL = (SELECT REPLACE('ERROR: Backing up database ' + @DatabaseName + ' : differential.' +ISNULL(ERROR_MESSAGE ( ),''),CHAR(10),'<br^>'));

			END CATCH
		END;
		ELSE IF @TypeOfBackup = 'trn'
		BEGIN;
			SET @SQL = 'USE [master]; BACKUP LOG ['	  + @DatabaseName + '] TO DISK = ''' + @Directory + @DatabaseName + '_backup_' + @backupfiledate + @backupfileextention+ ''' WITH STATS = 1' + CASE WHEN @WithCompression = 1 THEN ', COMPRESSION' ELSE '' END + CASE WHEN @MaxTransferSize > 0 and @is_encrypted = 1 and @encryption_state = 3 THEN ', MAXTRANSFERSIZE = ' + CAST(@MaxTransferSize as VARCHAR(20)) ELSE '' END + CASE WHEN @CopyOnly = 1 THEN ', COPY_ONLY' ELSE '' END;
			IF NOT EXISTS(SELECT * FROM Sys.Databases WHERE [Name] = @DatabaseName AND recovery_model = 3) -- recovery_model = 3 is simple recovery model
			BEGIN;
				BEGIN TRY;
					EXECUTE(@SQL)
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


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[Sys_BackupAllDatabases]
(
	@Directory VARCHAR(8000),
	@TypeOfBackup VARCHAR(5),
	@DatabaseList NVARCHAR(max) = 'ALL_DATABASES',
	@WithCompression BIT = 1,
	@MaxTransferSize INT = 262144,
	@CopyOnly BIT = 0
)
AS
SET NOCOUNT ON;

DECLARE @database_id INT,
		@DatabaseName SYSNAME,
		@is_encrypted BIT,
		@encryption_state INT,
		@ERRORVAL VARCHAR(2048),
		@SQL VARCHAR(2048)

DECLARE @tmpDatabases TABLE (ID int IDENTITY,
                           database_id int,
						   DatabaseName nvarchar(max),
                           DatabaseType nvarchar(max),
                           Selected bit,
						   is_encrypted bit,
						   encryption_state int,
                           PRIMARY KEY(Selected,ID))

DECLARE @SelectedDatabases TABLE (DatabaseName nvarchar(max),
                                DatabaseType nvarchar(max),
                                Selected bit)

SET @DatabaseList = REPLACE(@DatabaseList, CHAR(10), '')
SET @DatabaseList = REPLACE(@DatabaseList, CHAR(13), '')

WHILE CHARINDEX(', ',@DatabaseList) > 0 SET @DatabaseList = REPLACE(@DatabaseList,', ',',')
WHILE CHARINDEX(' ,',@DatabaseList) > 0 SET @DatabaseList = REPLACE(@DatabaseList,' ,',',')

SET @DatabaseList = LTRIM(RTRIM(@DatabaseList));

WITH Databases1 (StartPosition, EndPosition, DatabaseItem) AS
(
SELECT 1 AS StartPosition,
     ISNULL(NULLIF(CHARINDEX(',', @DatabaseList, 1), 0), LEN(@DatabaseList) + 1) AS EndPosition,
     SUBSTRING(@DatabaseList, 1, ISNULL(NULLIF(CHARINDEX(',', @DatabaseList, 1), 0), LEN(@DatabaseList) + 1) - 1) AS DatabaseItem
WHERE @DatabaseList IS NOT NULL
UNION ALL
SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
     ISNULL(NULLIF(CHARINDEX(',', @DatabaseList, EndPosition + 1), 0), LEN(@DatabaseList) + 1) AS EndPosition,
     SUBSTRING(@DatabaseList, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(',', @DatabaseList, EndPosition + 1), 0), LEN(@DatabaseList) + 1) - EndPosition - 1) AS DatabaseItem
FROM Databases1
WHERE EndPosition < LEN(@DatabaseList) + 1
),
Databases2 (DatabaseItem, Selected) AS
(
SELECT CASE WHEN DatabaseItem LIKE '-%' THEN RIGHT(DatabaseItem,LEN(DatabaseItem) - 1) ELSE DatabaseItem END AS DatabaseItem,
     CASE WHEN DatabaseItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
FROM Databases1
),
Databases3 (DatabaseItem, DatabaseType, Selected) AS
(
SELECT CASE WHEN DatabaseItem IN('ALL_DATABASES','SYSTEM_DATABASES','USER_DATABASES') THEN '%' ELSE DatabaseItem END AS DatabaseItem,
     CASE WHEN DatabaseItem = 'SYSTEM_DATABASES' THEN 'S' WHEN DatabaseItem = 'USER_DATABASES' THEN 'U' ELSE NULL END AS DatabaseType,
     Selected
FROM Databases2
),
Databases4 (DatabaseName, DatabaseType, Selected) AS
(
SELECT CASE WHEN LEFT(DatabaseItem,1) = '[' AND RIGHT(DatabaseItem,1) = ']' THEN PARSENAME(DatabaseItem,1) ELSE DatabaseItem END AS DatabaseItem,
     DatabaseType,
     Selected
FROM Databases3
)
INSERT INTO @SelectedDatabases (DatabaseName, DatabaseType, Selected)
SELECT DatabaseName,
     DatabaseType,
     Selected
FROM Databases4
OPTION (MAXRECURSION 0)

INSERT INTO @tmpDatabases (database_id, DatabaseName, DatabaseType, Selected, is_encrypted, encryption_state)
SELECT db.database_id,
	   [name] AS DatabaseName,
       CASE WHEN name IN('master','msdb','model') THEN 'S' ELSE 'U' END AS DatabaseType,
       0 AS Selected,
	   is_encrypted,
	   ISNULL(dm.encryption_state,-1) as encryption_state
FROM sys.databases db
LEFT OUTER JOIN sys.dm_database_encryption_keys dm
ON db.database_id = dm.database_id
WHERE [name] <> 'tempdb'
AND source_database_id IS NULL
ORDER BY [name] ASC

UPDATE tmpDatabases
SET tmpDatabases.Selected = SelectedDatabases.Selected
FROM @tmpDatabases tmpDatabases
INNER JOIN @SelectedDatabases SelectedDatabases
ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
WHERE SelectedDatabases.Selected = 1

DECLARE Backup_Cursor CURSOR LOCAL STATIC FOR
	SELECT database_id, DatabaseName, is_encrypted, encryption_state
	FROM @tmpDatabases
	WHERE Selected = 1
	-- Exclude databases with Log shipping
	AND DatabaseName NOT IN (SELECT primary_database
	FROM msdb.dbo.log_shipping_primary_databases);

OPEN Backup_Cursor;
FETCH NEXT FROM Backup_Cursor INTO @database_id, @DatabaseName, @is_encrypted, @encryption_state;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		-- Check if database is HADR and is primary
		DECLARE @Result BIT
		EXEC dbo.Sys_CheckHADR @DatabaseName = @DatabaseName , @Result = @Result OUTPUT
		IF (@Result = 1 AND @CopyOnly = 0) OR (@Result= 0 AND @CopyOnly = 1)
		BEGIN
			BEGIN TRY
				-- do backup
				EXEC dbo.Sys_CreateBackup @Directory = @Directory,
									@DatabaseName = @DatabaseName,
									@TypeOfBackup = @TypeOfBackup,
									@WithCompression = @WithCompression,
									@MaxTransferSize = @MaxTransferSize,
									@is_encrypted = @is_encrypted,
									@encryption_state = @encryption_state,
									@CopyOnly = @CopyOnly;

				IF @TypeOfBackup = 'trn' and @database_id > 4
				BEGIN
					EXEC dbo.Sys_ShrinkLog @DatabaseName
				END

			END TRY
			BEGIN CATCH
				SET @ERRORVAL= CONVERT(VARCHAR(2048),ISNULL(ERROR_MESSAGE ( ),''));
				SELECT @ERRORVAL;
				SET @ERRORVAL = (SELECT REPLACE(@ERRORVAL,CHAR(10),'<br^>'));

			END CATCH
		END
		FETCH NEXT FROM Backup_Cursor INTO @database_id, @DatabaseName, @is_encrypted, @encryption_state;
	END;
CLOSE Backup_Cursor;
DEALLOCATE Backup_Cursor;
GO
