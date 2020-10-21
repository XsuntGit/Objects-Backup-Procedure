USE [XsuntAdmin]
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
