USE [msdb]
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
			DECLARE @is_primary_replica SMALLINT
			SELECT @is_primary_replica = SUM(CAST(drs.is_primary_replica as INT))
			FROM sys.dm_hadr_database_replica_states AS drs
				JOIN sys.databases AS db
				ON drs.database_id = db.database_id
				LEFT OUTER JOIN sys.dm_hadr_availability_group_states AS gs
				ON gs.group_id = drs.group_id
			WHERE Name = @DatabaseName

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
