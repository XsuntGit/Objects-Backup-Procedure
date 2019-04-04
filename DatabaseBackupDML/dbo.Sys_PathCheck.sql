USE [msdb]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[Sys_PathCheck]
(
	@Directory VARCHAR(4000) ,
	@file_exists INT OUTPUT,
	@directory_exists INT OUTPUT,
	@parent_directory_exists INT OUTPUT
	)
AS
SET NOCOUNT ON;

CREATE TABLE #PathCheck ( file_exists BIT, directory_exists BIT, parent_directory_exists BIT );

INSERT INTO #PathCheck EXEC master.dbo.xp_fileexist @Directory

SELECT	@file_exists=file_exists,
			@directory_exists = directory_exists,
			@parent_directory_exists = parent_directory_exists
FROM #PathCheck;

DROP TABLE #PathCheck
