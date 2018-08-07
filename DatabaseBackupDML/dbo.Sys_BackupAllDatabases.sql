USE [msdb]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[Sys_BackupAllDatabases]
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
