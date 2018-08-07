USE [msdb]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[Sys_CheckHADR_Databases]
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
