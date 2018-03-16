USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[Sys_Restore_Object]
(
	@DatabaseName SYSNAME,
	@SchemaName SYSNAME,
	@TableName SYSNAME,
	@FilePath NVARCHAR(256),
	@Password VARCHAR(15)
)
AS
BEGIN
	SET NOCOUNT ON;
	BEGIN TRY

		DECLARE	@FullObjectName NVARCHAR(256)
		SET @FullObjectName = QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)

		IF OBJECT_ID(@FullObjectName, N'U') IS NOT NULL
		BEGIN

			DECLARE @cmd NVARCHAR(4000),
				@TestPath BIT,
				@StorageLocation NVARCHAR(256) = '\\172.28.99.15\amg-obj-bck\',
				@InputTableName NVARCHAR(256), 
				@OutputIdentColumn NVARCHAR(256),
				@ParamDefinition NVARCHAR(500),
				@IdentColumn NVARCHAR(256)

			DECLARE @tmp_path TABLE ([output] VARCHAR(16))
			SET @cmd = 'powershell.exe Test-Path "' + @FilePath + '"'
			INSERT INTO @tmp_path
			EXEC master..xp_cmdshell @cmd
			SELECT TOP 1 @TestPath = [output]
			FROM @tmp_path
			WHERE [output] is not NULL

			IF @TestPath = 1
			BEGIN

				SET @StorageLocation = @StorageLocation + CAST(lower(NEWID()) as NVARCHAR(256)) + '\'
				--Extract files from zip
				SET @cmd = 'powershell C:\XsuntScripts\Derchive_Files.ps1 ' + @FilePath + ' ' + @StorageLocation + ' ' + @Password
				--print @cmd
				EXEC master..xp_cmdshell @cmd, no_output

				SET @cmd = N'SELECT @OutputIdentColumn = i.[name]' + CHAR(13) +
				'FROM ' + @DatabaseName + '.sys.schemas AS s' + CHAR(13) +
				'INNER JOIN ' + @DatabaseName + '.sys.tables AS t' + CHAR(13) +
				'ON s.[schema_id] = t.[schema_id]' + CHAR(13) +
				'INNER JOIN ' + @DatabaseName + '.sys.identity_columns i' + CHAR(13) +
				'ON i.[object_id] = t.[object_id]  WHERE t.[name] = @InputTableName'

				SET @ParamDefinition = N'@InputTableName NVARCHAR(256), @OutputIdentColumn NVARCHAR(256) OUTPUT'

				EXECUTE sp_executesql @cmd, @ParamDefinition, @InputTableName = @TableName, @OutputIdentColumn = @IdentColumn OUTPUT;  
				IF @IdentColumn IS NOT NULL
				BEGIN
					--bulk insert with identity

					SET @cmd = N'TRUNCATE TABLE ' + QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ';' + CHAR(13) +
					'BULK INSERT '  + QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + CHAR(13) +
					'FROM ''' + @StorageLocation + @TableName + '.txt''' + CHAR(13) +
					'WITH (FORMATFILE = ''' + @StorageLocation + @TableName + '.xml''' + ', KEEPIDENTITY, KEEPNULLS)'

					EXECUTE (@cmd)

				END
				IF @IdentColumn IS NULL
				BEGIN
					--bulk insert with no identity

					SET @cmd = N'TRUNCATE TABLE ' + QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ';' + CHAR(13) +
					'BULK INSERT '  + QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + CHAR(13) +
					'FROM ''' + @StorageLocation + @TableName + '.txt''' + CHAR(13) +
					'WITH (FORMATFILE = ''' + @StorageLocation + @TableName + '.xml''' + ', KEEPNULLS)'

					EXECUTE (@cmd)

				END

				SET @cmd = 'powershell Remove-Item ' + @StorageLocation + ' -Recurse'
				EXEC master..xp_cmdshell @cmd, no_output

			END
			ELSE
			BEGIN
				PRINT 'No such file exists. Please double check the file path.'
			END

		END
		ELSE
		BEGIN
			PRINT 'No such table exists or the wrong object names are specified or you do not have permissions. Please contact administrator.'
		END

	END TRY
	BEGIN CATCH

		DECLARE @errmsg   nvarchar(2048),
				@severity tinyint,
				@state    tinyint,
				@errno    int,
				@proc     sysname,
				@lineno   int
           
		SELECT @errmsg = error_message(), @severity = error_severity(),
				@state  = error_state(), @errno = error_number(),
				@proc   = error_procedure(), @lineno = error_line()
       
		IF @errmsg NOT LIKE '***%'
		BEGIN
			SELECT @errmsg = '*** ' + coalesce(quotename(@proc), '<dynamic SQL>') + 
							', Line ' + ltrim(str(@lineno)) + '. Errno ' + 
							ltrim(str(@errno)) + ': ' + @errmsg
		END
		RAISERROR('%s', @severity, @state, @errmsg)

	END CATCH

END