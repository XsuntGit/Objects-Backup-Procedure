USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[sys_database_backup]
(
	@DatabaseName NVARCHAR(256),
	@Path NVARCHAR(4000)
)
WITH ENCRYPTION
AS
BEGIN
SET NOCOUNT ON

IF @DatabaseName in ('master','model','msdb','TempDb','XsuntAdmin')
BEGIN
	SELECT 'System database cannot be backed up.' as [output_message]
	RETURN -1
END

IF EXISTS (SELECT * FROM sys.databases db WHERE db.[name] = @DatabaseName)
	BEGIN

		IF EXISTS (SELECT [file_or_directory_name] FROM master.sys.dm_os_enumerate_filesystem(@Path,'*')		)
		BEGIN

			DECLARE @SQL NVARCHAR(2000),
					@ServerName VARCHAR(128),
					@FullPath NVARCHAR(256),
					@BackupFileDate NVARCHAR(50),
					@is_encrypted BIT,
					@encryption_state INT,
					@MaxTransferSize INT = 262144
			SELECT @is_encrypted = is_encrypted,
				@encryption_state = ISNULL(dm.encryption_state,-1)
			FROM sys.databases db
			LEFT OUTER JOIN sys.dm_database_encryption_keys dm
			ON db.database_id = dm.database_id
			WHERE db.[name] = @DatabaseName

			SET @BackupFileDate = REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(NVARCHAR(25),GETDATE(),121),'-','_'),' ' , '_'),':',''),'.','_')
			SET @ServerName = cast(@@SERVERNAME as VARCHAR(128))
			SET @FullPath = @Path + @DatabaseName + '_backup_' + @BackupFileDate + '.full'
			SET @SQL = N'SQLCMD -E -S ' + @ServerName + N' -Q "BACKUP DATABASE [' + @DatabaseName + '] TO DISK = ''' + @FullPath + ''' WITH STATS = 1, COMPRESSION, COPY_ONLY' + CASE WHEN @MaxTransferSize > 0 and @is_encrypted = 1 and @encryption_state = 3 THEN ', MAXTRANSFERSIZE = ' + CAST(@MaxTransferSize as VARCHAR(20)) ELSE '' END + '"'
			BEGIN TRY

				EXEC master.dbo.xp_cmdshell @SQL

				SELECT 'Database has been backed up successfully into the following file: ' + @FullPath  as [output_message]

			END TRY
			BEGIN CATCH

				SELECT 'ERROR: Backing up database ' + @DatabaseName + ' : full'  as [output_message]

			END CATCH

		END
		ELSE
		BEGIN

			SELECT 'ERROR: Path does not exist.' as [output_message]

		END
	END
	ELSE
	BEGIN

		SELECT 'ERROR: Database does not exist.' as [output_message]

	END
END
GO

EXEC [dbo].[SP_MS_MARKSYSTEMOBJECT] [sys_database_backup]
GO
