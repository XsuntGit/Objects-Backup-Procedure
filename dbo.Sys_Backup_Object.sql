/****** Object:  StoredProcedure [dbo].[Sys_Backup_Object]    Script Date: 2/26/2018 7:57:04 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[Sys_Backup_Object]
(
	@DatabaseName SYSNAME,
	@SchemaName SYSNAME,
	@TableName SYSNAME,
	@FilePath NVARCHAR(256)
)
AS
BEGIN
	SET NOCOUNT ON;

	BEGIN TRY

		DECLARE	@FullObjectName NVARCHAR(256)
		SET @FullObjectName = QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)

		IF OBJECT_ID(@FullObjectName, N'U') IS NOT NULL
		BEGIN

			DECLARE @cmd nvarchar(4000),
				@Server SYSNAME,
				@FilePathTemp NVARCHAR(256),
				@GUID UNIQUEIDENTIFIER,
				@EmailBody NVARCHAR(MAX),
				@EmailSubject NVARCHAR(100),
				@Now VARCHAR(50),
				@Passw VARCHAR(15),
				@nt_user_name NVARCHAR(256),
				@RecipientName VARCHAR(256),
				@RecipientEmail VARCHAR(256)

			SET @Now = REPLACE(CONVERT(VARCHAR(10), GETDATE(), 112), '/', '') + REPLACE(CONVERT(VARCHAR(10), GETDATE(), 108) , ':', '')
			SET @Server = @@SERVERNAME
			SET @GUID = NEWID()
			SET @FilePathTemp = @FilePath + cast(lower(@GUID) as NVARCHAR(256))
			SET @EmailSubject = @FullObjectName + ' object backup from ' + cast(GETDATE() as varchar(20))

			SELECT @nt_user_name = nt_user_name
			FROM sys.dm_exec_sessions 
			WHERE session_id = @@spid

			DECLARE @tbl_RecipientName TABLE ([output] VARCHAR(256))
			SET @cmd = 'powershell.exe Get-ADUser "' + CAST(@nt_user_name as VARCHAR(256)) + '"  -Properties mail'
			--print @cmd
			INSERT INTO @tbl_RecipientName
			EXEC master..xp_cmdshell @cmd
	
			SELECT @RecipientName = [output]
			FROM @tbl_RecipientName
			WHERE [output] like 'GivenName%'

			SELECT @RecipientEmail = [output]
			FROM @tbl_RecipientName
			WHERE [output] like 'mail%'

			SET @RecipientEmail = SUBSTRING(@RecipientEmail,charindex(':',@RecipientEmail)+2,LEN(@RecipientEmail))

			DECLARE @tmp_passw TABLE ([output] VARCHAR(16))
			INSERT INTO @tmp_passw
			EXEC master..xp_cmdshell 'powershell.exe "([char[]]([char]65..[char]90) + ([char[]]([char]97..[char]122)) + 0..9 | sort {Get-Random})[0..15] -join ''''"'
			SELECT TOP 1 @Passw = [output]
			FROM @tmp_passw
			WHERE [output] is not NULL

			SET @EmailBody = '<p><span style="color: #b20838;font-family: Consolas; font-size: 12px;">Hi ' + ISNULL(SUBSTRING(@RecipientName,charindex(':',@RecipientName)+2,LEN(@RecipientName)),'User') + ',</p>'
			SET @EmailBody = @EmailBody + 'The object <b>' + @FullObjectName + '</b> has been backed up successfully and it will be stored by the following location:<br/>'
			SET @EmailBody = @EmailBody + @FilePath + @TableName + '_' + @Now + '.zip' + '<br/>'
			SET @EmailBody = @EmailBody + 'Passw: ' + @Passw
			SET @EmailBody = @EmailBody + '<p>Here is the list of files included:</p>'
			SET @EmailBody = @EmailBody + '<b>' + @TableName + '.sql</b>' + '&emsp; - t-sql file with the table definition;' + '<br/>'
			SET @EmailBody = @EmailBody + '<b>' + @TableName + '.txt</b>' + '&emsp; - data flat file (no header);' + '<br/>'
			SET @EmailBody = @EmailBody + '<b>' + @TableName + '.xml</b>' + '&emsp; - format file to be used for bcp the data in (or <a href="https://docs.microsoft.com/en-us/sql/relational-databases/import-export/use-a-format-file-to-bulk-import-data-sql-server">BULK INSERT</a> statement).' + '<br/>'

			--Output tables DDL
			SET @cmd = 'powershell C:\XsuntScripts\Script_Out_DDL.ps1 ' + @Server + ' ' + @DatabaseName + ' ' + @SchemaName + ' ' + @TableName + ' ' + @FilePathTemp
			--print @cmd
			EXEC master..xp_cmdshell @cmd, no_output

			--Create format file for a table
			SET @cmd = 'bcp ' + @FullObjectName + ' format nul -c -x -t"|" -f "' + @FilePathTemp + '\' + @TableName +'.xml" -T -S ' + @Server
			--print @cmd
			EXEC master..xp_cmdshell @cmd, no_output

			--bcp table out into file
			SET @cmd = 'bcp "SELECT * FROM ' + @FullObjectName + '" queryout "' + @FilePathTemp  + '\' + @TableName + '.txt' + ' " /t "|" -T -c -C ACP -S ' + @Server + '"'
			--print @cmd
			EXEC master..xp_cmdshell @cmd, no_output

			--bcp table out into file
			SET @cmd = 'powershell C:\XsuntScripts\Archive_Files.ps1 "' + @FilePathTemp + '\*.*" "' + @FilePath + @TableName + '_' + @Now + '.zip" "' + @Passw + '"'
			--print @cmd
			EXEC master..xp_cmdshell @cmd, no_output

			--drop working folder
			SET @cmd = 'powershell Remove-Item ' + @FilePathTemp + ' -Recurse'
			--print @cmd
			EXEC master..xp_cmdshell @cmd, no_output

			--send out email notification
			EXEC msdb.dbo.sp_send_dbmail
				@profile_Name = 'Amg SQL Admin',
				@recipients = @RecipientEmail,
				@body_format='HTML',
				@body = @EmailBody,
				@subject = @EmailSubject

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
