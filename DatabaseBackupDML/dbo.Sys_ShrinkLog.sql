USE [msdb]
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
