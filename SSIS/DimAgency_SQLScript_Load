DROP TABLE IF EXISTS DimAgency
GO
CREATE TABLE DimAgency --- Type SCD 1
(
	AgencyKey			INT IDENTITY PRIMARY KEY,
	AgencyID			INT,
	AgencyName			VARCHAR(255) NULL,
	AgencyDescription	VARCHAR(255) NULL,
    UpdateDate          DATETIME NOT NULL
);
GO

DROP TABLE IF EXISTS Agency_Stage
GO
CREATE TABLE Agency_Stage 
(
	
	AgencyID			INT,
	AgencyName			VARCHAR(255),
	AgencyDescription	VARCHAR(255),
    UpdateDate          DATETIME 
);
GO

CREATE OR ALTER PROCEDURE Extract_Agency
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;
    INSERT INTO Agency_Stage (AgencyID, AgencyName, AgencyDescription, UpdateDate)
    SELECT DISTINCT ID,
					AgencyName, 
					AgencyDescription, 
					GETDATE()
	   FROM [NYC_311].[dbo].Agency
    	
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC Extract_Agency;
GO
SELECT * FROM Agency_Stage

--1. INITIAL LOAD
SELECT * FROM Agency_Stage
SELECT * FROM DimAgency


--new insertion in the staging table
INSERT INTO Agency_Stage (AgencyID, AgencyName, AgencyDescription, UpdateDate)
VALUES (16,'ABC','AGENCY DESCRIPTION',GETDATE())
SELECT * from Agency_Stage
SELECT * FROM DimAgency


-- Updating existing Records
UPDATE Agency_Stage SET AgencyName='DEF' WHERE AgencyID=16
SELECT * from Agency_Stage
SELECT * from Agency_Stage
SELECT * FROM DimAgency

