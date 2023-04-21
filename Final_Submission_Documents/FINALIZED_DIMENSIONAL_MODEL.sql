CREATE DATABASE NYC_311_REQUESTS_DM;
GO

--DROP DATABASE NYC_311_REQUESTS_DM;
--GO

USE NYC_311_REQUESTS_DM;
GO


-- ================================ DIMESNIONAL MODEL =======================================

-- ======= DimDate ============
DROP TABLE IF EXISTS DimDate
GO

CREATE TABLE DimDate		---SCD TYPE 0
(
   DateKey		INT PRIMARY KEY,
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
   TimeKey     INT PRIMARY KEY,
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
	-- SET @RowCt += @@ROWCOUNT;

    -- IF @RowCt = 0
    --     BEGIN
    --         THROW 50001, 'No records found. Check with source system.', 1;
    --     END
    COMMIT TRANSACTION;
END
GO

EXEC PopulateDimTimeForDay;
GO

SELECT * FROM DimTime;
GO
-- ======= DimLocation ============
DROP TABLE IF EXISTS DimComplaintLocation
GO
CREATE TABLE DimComplaintLocation --- Type SCD 0
(
	LocationKey			INT PRIMARY KEY,
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
        CC.District,
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
    LocationKey         INT PRIMARY KEY,
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
CREATE OR ALTER PROCEDURE Transform_ComplaintLocation
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
END
GO

EXEC Transform_ComplaintLocation;
GO

SELECT * FROM Preload_ComplaintLocation;
GO

---===================== LOAD DIMENSION COMPLAINT LOCATION =====================---

CREATE OR ALTER PROCEDURE Load_ComplaintLocation
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

-- ======= DimAgency ============

DROP TABLE IF EXISTS DimAgency
GO
CREATE TABLE DimAgency --- Type SCD 1
(
	AgencyKey			INT IDENTITY PRIMARY KEY,
	AgencyID			INT NOT NULL,
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
GO

--================= SSIS TESTING ============================================
----1. INITIAL LOAD
--SELECT * FROM Agency_Stage
--SELECT * FROM DimAgency


----new insertion in the staging table
--INSERT INTO Agency_Stage (AgencyID, AgencyName, AgencyDescription, UpdateDate)
--VALUES (16,'ABC','AGENCY DESCRIPTION',GETDATE())
--SELECT * from Agency_Stage
--SELECT * FROM DimAgency


---- Updating existing Records
--UPDATE Agency_Stage SET AgencyName='DEF' WHERE AgencyID=16
--SELECT * from Agency_Stage
--SELECT * FROM DimAgency
--=========================== X ====================================== X ==================================


-- ======= DimComplaintType ============

DROP TABLE IF EXISTS DimComplaintType
GO
CREATE TABLE DimComplaintType --- Type SCD 1
(
	ComplaintTypeKey		INT IDENTITY PRIMARY KEY,
	ComplaintTypeID			INT NOT NULL,
	ComplaintType			VARCHAR(255) NULL,
	ComplaintDescription	VARCHAR(500) NULL,
    UpdateDate              DATETIME NOT NULL
);
GO

DROP TABLE IF EXISTS DimComplaintType_Stage;
GO
CREATE TABLE DimComplaintType_Stage 
(
    ComplaintTypeID         INT,
    ComplaintType           VARCHAR(255),
    ComplaintDescription    VARCHAR(500),
	UpdateDate              DATETIME
);
GO

CREATE OR ALTER PROCEDURE Extract_DimComplaintType AS
BEGIN
	SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

	BEGIN TRANSACTION;

    -- Load data into Stage_DimComplaintType
    INSERT INTO DimComplaintType_Stage (ComplaintTypeID, ComplaintType, ComplaintDescription, UpdateDate)
    SELECT DISTINCT ID, ComplaintType, ComplaintDescriptor, GETDATE() FROM [NYC_311].[dbo].Complaint;
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC Extract_DimComplaintType;
GO

SELECT * FROM DimComplaintType_Stage;


--================= SSIS TESTING ============================================

----DELETE FROM DimComplaintType_Stage; 

----1. INITIAL LOAD
--SELECT * FROM DimComplaintType_Stage
--SELECT * FROM DimComplaintType


----new insertion in the staging table
--INSERT INTO DimComplaintType_Stage (ComplaintTypeID, ComplaintType, ComplaintDescription, UpdateDate)
--VALUES (964,'complainttype','ComplaintDescription',GETDATE())
--SELECT * from DimComplaintType_Stage
--SELECT * FROM DimComplaintType


---- Updating existing Records
--UPDATE DimComplaintType_Stage SET ComplaintDescription='Complaint Description is not enough' WHERE ComplaintTypeID=964
--SELECT * from DimComplaintType_Stage
--SELECT * from DimComplaintType
--=========================== X ====================================== X ==================================


-- ======= DimStatus ============

DROP TABLE IF EXISTS DimStatus
GO
CREATE TABLE DimStatus --- Type SCD 2
(
	StatusKey							INT IDENTITY PRIMARY KEY,
    StatusID                            INT NOT NULL,
	StatusType							VARCHAR(255) NULL,
    StatusResolutionDescription			VARCHAR(1000) NULL,
    StatusDurationDays					INT NULL,
	StatusStarted						DATETIME NULL,
    StatusUpdatedDate                   DATETIME NULL,
    StatusEnded						    DATETIME NULL, ---> StatusResolutionActionUpdatedDate
	StartDate							DATE NOT NULL,
    EndDate								DATE NULL
);
GO

DROP TABLE IF EXISTS Status_Stage
GO
CREATE TABLE Status_Stage
(
    StatusID                            INT,
	StatusType							VARCHAR(255),
    StatusResolutionDescription			VARCHAR(1000),
    StatusDurationDays					INT,
	StatusStarted						DATETIME, ---> Created Date from Service Request
    StatusUpdatedDate                   DATETIME,  ---> StatusResolutionActionUpdatedDate
    StatusEnded						    DATETIME, --> End Date from Service Request
);
GO


CREATE OR ALTER PROCEDURE [dbo].[Extract_Status]
AS
BEGIN
	SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;
    
	INSERT INTO Status_Stage (StatusID, StatusType, StatusResolutionDescription,
    StatusDurationDays, StatusStarted, StatusUpdatedDate, StatusEnded)
    SELECT DISTINCT TOP(100000)
        s.ServiceKeyID,
        r.ResolutionStatus,
        r.ResolutionDescription,
        DATEDIFF(DAY, s.CreatedDate, s.ClosedDate) AS StatusDurationDays,
        s.CreatedDate,
        r.ResolutionActionUpdatedDate AS StatusUpdatedDate,
        s.ClosedDate
    FROM [NYC_311].[dbo].ServiceRequest s
    INNER JOIN [NYC_311].[dbo].Resolution r ON s.ResolutionID = r.ID 
	and r.ResolutionStatus IN ('Assigned','Closed','In Progress', 'Open', 'Pending', 'Started', 'Unspecified')
	
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC Extract_Status;
GO


SELECT * FROM Status_Stage;
GO
SELECT * FROM DimStatus
GO


----=========================== SSIS TESTING STARTED =============================================

--SELECT * FROM DimStatus where statusID=56922572 
--GO

----1. New Insertion 
--INSERT INTO Status_Stage (StatusID, StatusType) values (11223344,'Open')
--SELECT * FROM Status_Stage WHERE StatusID=11223344

--SELECT * FROM DimStatus

----2. STATUS got changed from 'Open' to 'In Progress' , StatusStarted to getdate()
--UPDATE Status_Stage SET  StatusType='In Progress', StatusStarted=GETDATE() WHERE StatusID=11223344
--SELECT * FROM Status_Stage WHERE StatusID=11223344
--SELECT * FROM DimStatus WHERE StatusID=11223344

----=========================== ANALYSIS ENDED =============================================

--Fact Table:

-- ======= FactComplaint ============
DROP TABLE IF EXISTS Stage_Complaint
GO
CREATE TABLE
    Stage_Complaint (
        ServiceRequestID INT,
        StatusStarted DATETIME,
        LocationZip VARCHAR(255),
        LocationAddress VARCHAR(255),
        AgencyID INT,
        ComplaintTypeID INT,
        StatusID INT
    );

GO

CREATE OR ALTER PROCEDURE Extract_Complaint
AS
BEGIN
    INSERT INTO Stage_Complaint (ServiceRequestID, StatusStarted,
     LocationZip, LocationAddress, AgencyID, ComplaintTypeID,StatusID)
    SELECT TOP(100000)
        sr.ServiceKeyID,
        sr.CreatedDate,
        il.IncidentZip,
        il.IncidentAddress,
        sr.AgencyID,
        sr_c.ComplaintID,
        sr.ResolutionID
    FROM
        [NYC_311].[dbo].ServiceRequest sr
        JOIN [NYC_311].[dbo].IncidentLocations il ON sr.LocationID = il.ID
        JOIN [NYC_311].[dbo].ServiceRequest_Complaint sr_c ON sr.UniqueKey = sr_c.ServiceRequestID
        JOIN [NYC_311].[dbo].Complaint c ON sr_c.ComplaintID = c.ID
        JOIN [NYC_311].[dbo].Resolution r ON sr.ResolutionID = r.ID;
END
GO

EXEC Extract_Complaint;

SELECT * FROM Stage_Complaint;
GO

DROP TABLE IF EXISTS Preload_Complaint
GO
CREATE TABLE Preload_Complaint 
(
    DateKey						INT NOT NULL ,
	TimeKey						INT NOT NULL ,
    LocationKey					INT NOT NULL ,
    AgencyKey					INT NOT NULL ,
    ComplaintTypeKey			INT NOT NULL,
    StatusKey					INT NOT NULL ,
    Total_Complaints			INT NOT NULL,
    Total_Resolved_Complaints	INT	NOT NULL,
    Total_Unresolved_Complaints INT	NOT NULL,
    Avg_Resolution_Time_Hours	FLOAT 
);
GO


CREATE OR ALTER PROCEDURE Transform_Complaint 
AS 
BEGIN
-- Insert data from Stage_Complaint into Preload_Complaint
INSERT INTO
    Preload_Complaint (
        DateKey,
        TimeKey,
        LocationKey,
        AgencyKey,
        ComplaintTypeKey,
        StatusKey,
        Total_Complaints,
        Total_Resolved_Complaints,
        Total_Unresolved_Complaints,
        Avg_Resolution_Time_Hours
    )

SELECT TOP(10000000)
    d.DateKey,
    t.TimeKey,
    l.LocationKey,
    a.AgencyKey,
    ct.ComplaintTypeKey,
    s.StatusKey,
    COUNT(*) AS Total_Complaints,
    SUM(
        CASE
            WHEN s.StatusType = 'Closed' THEN 1
            ELSE 0
        END
    ) AS Total_Resolved_Complaints,
    SUM(
        CASE
            WHEN s.StatusType != 'Closed' THEN 1
            ELSE 0
        END
    ) AS Total_Unresolved_Complaints,
    AVG(DATEDIFF (HOUR, s.StatusStarted, s.StatusEnded)) AS Avg_Resolution_Time_Hours
FROM
    Stage_Complaint sc
    INNER JOIN [NYC_311_REQUESTS_DM].[dbo].DimDate d ON CONVERT(DATE, sc.StatusStarted) = d.aDate
    INNER JOIN [NYC_311_REQUESTS_DM].[dbo].DimTime t ON CONVERT(TIME, sc.StatusStarted) = t.aTime
    INNER JOIN [NYC_311_REQUESTS_DM].[dbo].DimComplaintLocation l ON sc.LocationZip = l.Zip
    AND sc.LocationAddress = l.LocationAddress
    INNER JOIN [NYC_311_REQUESTS_DM].[dbo].DimAgency a ON sc.AgencyID = a.AgencyID
    INNER JOIN [NYC_311_REQUESTS_DM].[dbo].DimComplaintType ct ON sc.ComplaintTypeID = ct.ComplaintTypeID
    INNER JOIN [NYC_311_REQUESTS_DM].[dbo].DimStatus s ON sc.ServiceRequestID = s.StatusID

GROUP BY
    d.DateKey,
    t.TimeKey,
    l.LocationKey,
    a.AgencyKey,
    ct.ComplaintTypeKey,
    s.StatusKey

END 
GO

EXEC Transform_Complaint;
GO

SELECT * FROM Preload_Complaint;

DROP TABLE IF EXISTS FactComplaint
GO
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
    Avg_Resolution_Time_Hours	FLOAT
);
GO

CREATE OR ALTER PROCEDURE Load_Complaint
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO FactComplaint (DateKey, TimeKey, LocationKey, AgencyKey, ComplaintTypeKey, StatusKey, Total_Complaints, Total_Resolved_Complaints, Total_Unresolved_Complaints, Avg_Resolution_Time_Hours)
    SELECT P.DateKey, P.TimeKey, p.LocationKey, p.AgencyKey, p.ComplaintTypeKey, p.StatusKey, p.Total_Complaints, p.Total_Resolved_Complaints, p.Total_Unresolved_Complaints, p.Avg_Resolution_Time_Hours
    FROM Preload_Complaint p
    
END

EXECUTE Load_Complaint;

SELECT * FROM FactComplaint;


CREATE INDEX IX_FactComplaint_FK ON FactComplaint(DateKey, LocationKey, AgencyKey, ComplaintTypeKey, StatusKey)
GO
