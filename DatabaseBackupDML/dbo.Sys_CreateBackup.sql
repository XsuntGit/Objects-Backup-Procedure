USE [XsuntAdmin]
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
