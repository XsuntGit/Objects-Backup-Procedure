USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[sys_database_restore]
(
	@DatabaseName NVARCHAR(256),
	@FilePath NVARCHAR(4000)
)
WITH ENCRYPTION
AS
BEGIN
SET NOCOUNT ON

IF @DatabaseName in ('master','model','msdb','TempDb','XsuntAdmin')
BEGIN
	SELECT 'System database cannot be restored.' as [output_message]
	RETURN -1
END

IF NOT EXISTS (SELECT * FROM sys.databases db WHERE db.[name] = @DatabaseName)
	BEGIN

		IF EXISTS (
			SELECT [file_or_directory_name] FROM master.sys.dm_os_enumerate_filesystem(left(@FilePath,len(@FilePath)+1 - charindex('\',reverse(@FilePath),0)),'*')
			WHERE [file_or_directory_name] = substring(@FilePath, len(@FilePath)+1 - charindex('\',reverse(@FilePath),0)+1, len(@FilePath))
		)
		BEGIN

			DECLARE @SQL NVARCHAR(2000),
					@ServerName VARCHAR(128),
					@CMD NVARCHAR(2000),
					@query NVARCHAR(1000),
					@LogicalName VARCHAR(256),
					@Type VARCHAR(1),
					@FileGroupId INT,
					@FileId INT
			DECLARE @sqlcmdoutput TABLE ([output] VARCHAR(1000))
			SET @ServerName = cast(@@SERVERNAME as VARCHAR(128))
			SET @query = N'DECLARE @fileListTable TABLE (LogicalName nvarchar(128),PhysicalName nvarchar(260),[Type] char(1),FileGroupName nvarchar(128),Size numeric(20,0),MaxSize numeric(20,0),FileID bigint,CreateLSN numeric(25,0),DropLSN numeric(25,0),UniqueID uniqueidentifier,ReadOnlyLSN numeric(25,0),ReadWriteLSN numeric(25,0),BackupSizeInBytes bigint,SourceBlockSize int,FileGroupID int,LogGroupGUID uniqueidentifier,DifferentialBaseLSN numeric(25,0),DifferentialBaseGUID uniqueidentifier,IsReadOnl bit,IsPresent bit,TDEThumbprint varbinary(32),SnapshotUrl nvarchar(360));INSERT INTO @fileListTable EXEC(''restore filelistonly from disk = N''''' + @FilePath + ''''''');SELECT LogicalName,[Type],FileGroupId,FileId FROM @fileListTable'
			SET @CMD = 'SQLCMD -E -S ' + @ServerName + ' -s "&" -W -h -1 -Q "' + @query + '"'
			INSERT INTO @sqlcmdoutput EXEC master.dbo.xp_cmdshell @CMD
			SET @query = 'RESTORE DATABASE [' + @DatabaseName + '] FROM DISK = ''' + @FilePath + ''' WITH STATS = 1'
			DECLARE filenames CURSOR FORWARD_ONLY LOCAL READ_ONLY FOR
			WITH tmp_output AS(
				SELECT [output],
					[value],
					ROW_NUMBER() OVER(PARTITION BY [output] ORDER BY (SELECT NULL)) as row_num
				FROM @sqlcmdoutput o
					CROSS APPLY STRING_SPLIT([output], '&')
				WHERE [output] like '%&%'
			)
			SELECT [1] as LogicalName,
				[2] as [Type],
				[3] as FileGroupId,
				[4] as FileId
			FROM tmp_output
			PIVOT(
				MAX([value])
				FOR row_num IN ([1],[2],[3],[4])
			) as pvt
			OPEN filenames
			FETCH NEXT FROM filenames INTO @LogicalName, @Type, @FileGroupId, @FileId
			WHILE @@FETCH_STATUS = 0
			BEGIN

				IF @Type = 'D'
				BEGIN
					IF @FileGroupId = 1 AND @FileId = 1
						SET @query = @query + ', MOVE ''' + @LogicalName + ''' TO ''' + cast(SERVERPROPERTY('InstanceDefaultDataPath') as VARCHAR(256)) + @DatabaseName + '_' + cast(@FileId as VARCHAR(10)) + '.mdf'''
					ELSE
						SET @query = @query + ', MOVE ''' + @LogicalName + ''' TO ''' + cast(SERVERPROPERTY('InstanceDefaultDataPath') as VARCHAR(256)) + @DatabaseName + '_' + cast(@FileId as VARCHAR(10)) + '.ndf'''
				END
				ELSE
				IF @Type = 'L'
				BEGIN
					SET @query = @query + ', MOVE ''' + @LogicalName + ''' TO ''' + cast(SERVERPROPERTY('InstanceDefaultLogPath') as VARCHAR(256)) + @DatabaseName + '_log' + '.ldf'''
				END
				ELSE
				IF @Type = 'S'
				BEGIN
					SET @query = @query + ', MOVE ''' + @LogicalName + ''' TO ''' + cast(SERVERPROPERTY('InstanceDefaultDataPath') as VARCHAR(256))
				END
				FETCH NEXT FROM filenames INTO @LogicalName, @Type, @FileGroupId, @FileId

			END
			CLOSE filenames
			DEALLOCATE filenames

			SET @CMD = 'SQLCMD -E -S ' + @ServerName + ' -Q "' + @query + '"'
			BEGIN TRY

				EXEC master.dbo.xp_cmdshell @CMD

				SELECT 'Database [' + @DatabaseName + '] has been restored successfully from the following file: ' + @FilePath  as [output_message]
			END TRY
			BEGIN CATCH

				SELECT 'ERROR: Restoring database [' + @DatabaseName + '] from file: '  + @FilePath as [output_message]

			END CATCH

		END
		ELSE
		BEGIN

			SELECT 'ERROR: Database backup file does not exist.' as [output_message]

		END
	END
	ELSE
	BEGIN

		SELECT 'ERROR: Database already exist.' as [output_message]

	END
END
GO

EXEC [dbo].[SP_MS_MARKSYSTEMOBJECT] [sys_database_restore]
GO
