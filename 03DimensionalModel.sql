CREATE DATABASE NYC_311_REQUESTS_DM;
GO

--DROP DATABASE NYC_311_REQUESTS_DM;
--GO

USE NYC_311_REQUESTS_DM;
GO


-- ================================ DIMESNIONAL MODEL =======================================

-- ======= DimDate ============

CREATE TABLE DimDate		---SCD TYPE 0
(
   DateKey		INT NOT NULL,
   aDate		DATE NOT NULL,
   aYear		INT NOT NULL,
   aQuarter		INT NOT NULL,
   aMonth		INT NOT NULL,
   aDay			INT NOT NULL,
   aDayOfWeek   INT NOT NULL,
);
GO

----================ PROCEDURE TO LOAD DIMDATE ==================----
CREATE OR ALTER PROCEDURE DimDateInsertion
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;
    -- Create a temporary table to hold the date range
    DECLARE @DateTable TABLE (aDate DATE);
    
    -- Insert the range of dates into the temporary table
    WHILE @StartDate <= @EndDate
    BEGIN
        INSERT INTO @DateTable (aDate) VALUES (@StartDate);
        SET @StartDate = DATEADD(day, 1, @StartDate);
    END
    SET @RowCt += @@ROWCOUNT;
    -- Insert the dates from the temporary table into the DimDate table
    INSERT INTO DimDate (DateKey, aDate, aYear, aQuarter, aMonth, aDay, aDayOfWeek)
    SELECT 
        CONVERT(INT, CONVERT(VARCHAR(8), aDate, 112)),
        aDate,
        YEAR(aDate),
        DATEPART(QUARTER, aDate),
        MONTH(aDate),
        DAY(aDate),
        DATEPART(WEEKDAY, aDate)
    FROM @DateTable;
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC DimDateInsertion '2023-01-01', '2023-03-31';
GO

SELECT * FROM DimDate order by datekey;
GO

-- ======= DimTime ============

CREATE TABLE DimTime  ---SCD TYPE 0
(
   TimeKey     INT NOT NULL,
   aTime       TIME(0) NOT NULL,
   anHour      INT NOT NULL,
   aMinute     INT NOT NULL,
);
GO


----================ PROCEDURE TO LOAD DIMTIME ==================----
CREATE OR ALTER PROCEDURE PopulateDimTimeForDay
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;
    DECLARE @date DATE = GETDATE();
    DECLARE @hour INT = 0;
    DECLARE @minute INT = 0;
	

    BEGIN TRANSACTION;
    WHILE @hour < 24
    BEGIN
        WHILE @minute < 60
        BEGIN
            INSERT INTO DimTime (TimeKey, aTime, anHour, aMinute)
            VALUES ((@hour * 60) + @minute, 
                    CAST(CAST(@date AS VARCHAR(10)) + ' ' + CAST(@hour AS VARCHAR(2)) + ':' + CAST(@minute AS VARCHAR(2)) AS DATETIME),
                    @hour, @minute);
            SET @minute = @minute + 1;
        END
        SET @hour = @hour + 1;
        SET @minute = 0;
    END
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC PopulateDimTimeForDay;
GO

SELECT * FROM DimTime;
GO
-- ======= DimLocation ============


CREATE TABLE DimComplaintLocation --- Type SCD 0
(
	LocationKey			INT NOT NULL,
    Zip			        VARCHAR(255)	NULL,
	LocationAddress		VARCHAR(255)    NULL,
    CityCouncilDistrict INT				NULL,
    PolicePrecinct		VARCHAR(255)	NULL,
    City				VARCHAR(255)	NULL,
    Borough				VARCHAR(50)	    NULL,
    LocationType		VARCHAR(255)	NULL
);
GO

--============== ETL FOR DimComplaintLocation ==================

--============== ComplaintLocation STAGE TABLE ==================

CREATE TABLE Stage_ComplaintLocation --- Type SCD 0
(
    Zip			        VARCHAR(255)	NULL,
	LocationAddress		VARCHAR(255)    NULL,
    CityCouncilDistrict INT				NULL,
    PolicePrecinct		VARCHAR(255)	NULL,
    City				VARCHAR(255)	NULL,
    Borough				VARCHAR(50)	    NULL,
    LocationType		VARCHAR(255)	NULL
);
GO


--============== EXTRACT STORED PROCEDURE ==================

CREATE OR ALTER PROCEDURE Extract_ComplaintLocation
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;
    INSERT INTO Stage_ComplaintLocation (Zip, LocationAddress, CityCouncilDistrict, PolicePrecinct, City, Borough, LocationType)
    SELECT DISTINCT
        IL.IncidentZip,
        IL.IncidentAddress,
        CC.BoroughID,
        PP.PrecinctName,
        C.CityName,
        BB.BoroughName,
        IL.AddressType
    FROM [NYC_311].[dbo].ServiceRequest SR
    JOIN [NYC_311].[dbo].IncidentLocations IL ON SR.LocationID = IL.ID
    LEFT JOIN [NYC_311].[dbo].City C ON SR.CityID = C.ID
    LEFT JOIN [NYC_311].[dbo].CityCouncilBoundary CC ON SR.CityCouncilDistrictID = CC.District
    LEFT JOIN [NYC_311].[dbo].BoroughBoundary BB ON SR.BoroughBoundaryID = BB.BoroughValue
    LEFT JOIN [NYC_311].[dbo].PolicePrecinct PP ON SR.PolicePrecinctID = PP.PrecinctNumber;
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC Extract_ComplaintLocation;
GO

SELECT * FROM Stage_ComplaintLocation
GO
---===================== COMPLAINT LOCATION PRELOAD STAGING TABLE =====================---

CREATE TABLE Preload_ComplaintLocation
(
    LocationKey         INT,
    Zip                 VARCHAR(255)    NULL,
    LocationAddress     VARCHAR(255)    NULL,
    CityCouncilDistrict INT             NULL,
    PolicePrecinct      VARCHAR(255)    NULL,
    City                VARCHAR(255)    NULL,
    Borough             VARCHAR(50)     NULL,
    LocationType        VARCHAR(255)    NULL
);
GO

--==== SEQUENCE CREATED ==========
CREATE SEQUENCE dbo.LocationKey START WITH 1;
GO


--======= TRANSFORM STORED PROCEDURE =============
CREATE PROCEDURE Transform_ComplaintLocation
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

	TRUNCATE TABLE Preload_ComplaintLocation;
    
	BEGIN TRANSACTION;
    
    -- Insert new rows with unique LocationKeys generated by the sequence
    INSERT INTO Preload_ComplaintLocation (LocationKey, Zip, LocationAddress, CityCouncilDistrict, PolicePrecinct, City, Borough, LocationType)
    SELECT NEXT VALUE FOR dbo.LocationKey, Zip, LocationAddress, CityCouncilDistrict, PolicePrecinct, City, Borough, LocationType
    FROM Stage_ComplaintLocation;
    
    -- No need to update existing rows in SCD type 0
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END;
GO

EXEC Transform_ComplaintLocation;
GO

SELECT * FROM Preload_ComplaintLocation;
GO

---===================== LOAD DIMENSION COMPLAINT LOCATION =====================---

CREATE PROCEDURE Load_ComplaintLocation
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    -- Delete all existing data from DimComplaintLocation
    DELETE FROM DimComplaintLocation;
    
	BEGIN TRANSACTION;
    -- Insert data from preload table
    INSERT INTO DimComplaintLocation (LocationKey, Zip, LocationAddress, CityCouncilDistrict, PolicePrecinct, City, Borough, LocationType)
    SELECT LocationKey, Zip, LocationAddress, CityCouncilDistrict, PolicePrecinct, City, Borough, LocationType
    FROM Preload_ComplaintLocation;
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC Load_ComplaintLocation;
GO

SELECT * FROM DimComplaintLocation;
GO
--============== ETL FOR Dimension Agency ==================

-- ======= DimAgency ============

CREATE TABLE Stage_DimAgency (
    AgencyID           INT,
    AgencyName          VARCHAR(255),
    AgencyDescription   VARCHAR(255),
    
);
GO

CREATE OR ALTER PROCEDURE Extract_Agency AS
BEGIN
    -- Load data into Stage_DimAgency
    INSERT INTO Stage_DimAgency (AgencyID, AgencyName, AgencyDescription)
    SELECT ID, AgencyName, AgencyDescription FROM [NYC_311].[dbo].Agency;

END;

EXEC Extract_Agency;
GO

SELECT * FROM Stage_DimAgency;


CREATE TABLE DimAgency --- Type SCD 1
(
	AgencyKey			INT NOT NULL,
	AgencyName			VARCHAR(255) NULL,
	AgencyDescription	VARCHAR(255) NULL,
    UpdateDate          DATETIME NOT NULL
);
GO

--============================ ETL FOR Dimension Complaint Type ================================

--============== ComplaintLocation STAGE TABLE ==================

DROP TABLE IF EXISTS Stage_DimComplaintType;
CREATE TABLE Stage_DimComplaintType (
    ComplaintTypeID        INT,
    ComplaintType           VARCHAR(255),
    ComplaintDescription    VARCHAR(MAX)
);
GO

CREATE OR ALTER PROCEDURE Extract_DimComplaintType AS
BEGIN
      -- Load data into Stage_DimComplaintType
    INSERT INTO Stage_DimComplaintType (  ComplaintTypeID,   ComplaintType, ComplaintDescription)
    SELECT ID, ComplaintType, ComplaintDescriptor FROM [NYC_311].[dbo].Complaint;

END;

EXEC Extract_DimComplaintType;
GO

SELECT * FROM Stage_DimComplaintType;
 
--============== ComplaintLocation DIMENSION TABLE =============

CREATE TABLE DimComplaintType --- Type SCD 1
(
	ComplaintTypeKey		INT NOT NULL,
	ComplaintType			VARCHAR(255) NULL,
	ComplaintDescription	VARCHAR(MAX) NULL,
    UpdateDate              DATETIME NOT NULL
);
GO

--============================ ETL FOR Dimension Status Type ================================

--============== STATUS STAGE TABLE ==================


CREATE TABLE Stage_Status--- Type SCD 2
(
    StatusID                            INT NOT NULL,
	StatusType							VARCHAR(255) NULL,
    StatusResolutionDescription			VARCHAR(MAX) NULL,
    StatusDurationDays					INT NULL,
	StatusStarted						DATETIME NULL, ---> Created Date from Service Request
    StatusUpdatedDate                   DATETIME NULL,  ---> StatusResolutionActionUpdatedDate
    StatusEnded						    DATETIME NULL, --> End Date from Service Request
);
GO


CREATE OR ALTER PROCEDURE [dbo].[Extract_Status]
AS
BEGIN
    INSERT INTO Stage_Status (StatusID, StatusType, StatusResolutionDescription,
     StatusDurationDays, StatusStarted, StatusUpdatedDate, StatusEnded)
    SELECT 
        s.ServiceKeyID,
        r.ResolutionStatus,
        r.ResolutionDescription,
        DATEDIFF(DAY, s.CreatedDate, COALESCE(s.ClosedDate, GETDATE())) AS StatusDurationDays,
        s.CreatedDate,
        r.ResolutionActionUpdatedDate AS StatusUpdatedDate,
        s.ClosedDate
    FROM [NYC_311].[dbo].ServiceRequest s
    LEFT JOIN [NYC_311].[dbo].Resolution r ON s.ResolutionID = r.ID;
END

--  CASE 
--             WHEN s.ClosedDate IS NULL THEN r.ResolutionActionUpdatedDate 
--             ELSE NULL 
--         END AS StatusUpdatedDate,

EXEC Extract_Status;

CREATE TABLE DimStatus --- Type SCD 2
(
	StatusKey							INT NOT NULL,
    StatusID                            INT NOT NULL,
	StatusType							VARCHAR(255) NULL,
    StatusResolutionDescription			VARCHAR(MAX) NULL,
    StatusDurationDays					INT NULL,
	StatusStarted						DATETIME NULL, ---> Created Date from Service Request
    StatusUpdatedDate                   DATETIME NULL,  ---> StatusResolutionActionUpdatedDate
    StatusEnded						    DATETIME NULL, --> End Date from Service Request
	StartDate							DATE NOT NULL,
    EndDate								DATE NULL
);
GO

--Fact Table:A

CREATE TABLE FactComplaint 
(
    DateKey						INT NOT NULL FOREIGN KEY REFERENCES DimDate(DateKey),
	TimeKey						INT NOT NULL FOREIGN KEY REFERENCES DimTime(TimeKey),
    LocationKey					INT NOT NULL FOREIGN KEY REFERENCES DimComplaintLocation(LocationKey),
    AgencyKey					INT NOT NULL FOREIGN KEY REFERENCES DimAgency(AgencyKey),
    ComplaintTypeKey			INT NOT NULL FOREIGN KEY REFERENCES DimComplaintType(ComplaintTypeKey),
    StatusKey					INT NOT NULL FOREIGN KEY REFERENCES DimStatus(StatusKey),
    Total_Complaints			INT NOT NULL,
    Total_Resolved_Complaints	INT	NOT NULL,
    Total_Unresolved_Complaints INT	NOT NULL,
    Total_Escalated_Complaints	INT	NOT NULL,
    Total_Reassigned_Complaints INT	NOT NULL,
    Avg_Resolution_Time_Hours	FLOAT NOT NULL,
    Escalation_Rate				FLOAT NOT NULL,
);
CREATE INDEX IX_FactComplaint_FK ON FactComplaint(DateKey, LocationKey, AgencyKey, ComplaintTypeKey, StatusKey)
GO



-- HOW TO GET THE Total_Complaints PER ROW:
/*
SELECT 
    DimDate.cdate AS Date,
    DimDate.chour AS Hour,
    DimLocation.IncidentZip AS ZipCode,
    DimAgency.AgencyName AS Agency,
    COUNT(*) AS Total_Complaints
FROM 
    FactComplaint
    JOIN DimDate ON FactComplaint.DateKey = DimDate.DateKey
    JOIN DimLocation ON FactComplaint.LocationKey = DimLocation.LocationKey
    JOIN DimAgency ON FactComplaint.AgencyKey = DimAgency.AgencyKey
GROUP BY 
    DimDate.cdate,
    DimDate.chour,
    DimLocation.IncidentZip,
    DimAgency.AgencyName

-- HOW TO GET THE Total_Resolved_Complaints PER ROW:
SELECT 
    DimDate.cdate AS Date,
    DimDate.chour AS Hour,
    DimLocation.IncidentZip AS ZipCode,
    DimAgency.AgencyName AS Agency,
    COUNT(CASE WHEN DimStatus.StatusName = 'Closed' THEN 1 ELSE NULL END) AS Total_Resolved_Complaints
FROM 
    FactComplaint
    JOIN DimDate ON FactComplaint.DateKey = DimDate.DateKey
    JOIN DimLocation ON FactComplaint.LocationKey = DimLocation.LocationKey
    JOIN DimAgency ON FactComplaint.AgencyKey = DimAgency.AgencyKey
    JOIN DimStatus ON FactComplaint.StatusKey = DimStatus.StatusKey
GROUP BY 
    DimDate.cdate,
    DimDate.chour,
    DimLocation.IncidentZip,
    DimAgency.AgencyName

-- HOW TO GET THE Total_Unresolved_Complaints PER ROW:
SELECT 
    DimDate.cdate AS Date,
    DimDate.chour AS Hour,
    DimLocation.IncidentZip AS ZipCode,
    DimAgency.AgencyName AS Agency,
    COUNT(CASE WHEN DimStatus.StatusName NOT IN ('Closed') THEN 1 ELSE NULL END) AS Total_Unresolved_Complaints
FROM 
    FactComplaint
    JOIN DimDate ON FactComplaint.DateKey = DimDate.DateKey
    JOIN DimLocation ON FactComplaint.LocationKey = DimLocation.LocationKey
    JOIN DimAgency ON FactComplaint.AgencyKey = DimAgency.AgencyKey
    JOIN DimStatus ON FactComplaint.StatusKey = DimStatus.StatusKey
GROUP BY 
    DimDate.cdate,
    DimDate.chour,
    DimLocation.IncidentZip,
    DimAgency.AgencyName

-- HOW TO GET THE Total_Escalated_Complaints PER ROW:
SELECT 
    DimDate.cdate AS Date,
    DimDate.chour AS Hour,
    DimLocation.IncidentZip AS ZipCode,
    DimAgency.AgencyName AS Agency,
    COUNT(CASE WHEN FactComplaint.Total_Escalated_Complaints > 0 THEN 1 ELSE NULL END) AS Total_Escalated_Complaints
FROM 
    FactComplaint
    JOIN DimDate ON FactComplaint.DateKey = DimDate.DateKey
    JOIN DimLocation ON FactComplaint.LocationKey = DimLocation.LocationKey
    JOIN DimAgency ON FactComplaint.AgencyKey = DimAgency.AgencyKey
GROUP BY 
    DimDate.cdate,
    DimDate.chour,
    DimLocation.LocationIncidentZip,
    DimAgency.AgencyName
*/

--DROP TABLE dbo.factcomplaint
--DROP TABLE dbo.dimAgency
--DROP TABLE dbo.dimDate
--DROP TABLE dbo.DimTime
--DROP TABLE dbo.dimstatus
--DROP TABLE dbo.dimlocation
--DROP TABLE dbo.dimcomplainttype
