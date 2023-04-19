CREATE DATABASE NYC_311_REQUESTS_DM;
DROP DATABASE NYC_311_REQUESTS_DM;
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
   aHour		INT NOT NULL
);
GO

CREATE OR ALTER PROCEDURE InsertDimDateFromRange (  @StartDate DATE, @EndDate DATE)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CurrentDate DATE = @StartDate

    WHILE @CurrentDate <= @EndDate
    BEGIN
        DECLARE @Year INT = YEAR(@CurrentDate)
        DECLARE @Quarter INT = DATEPART(QUARTER, @CurrentDate)
        DECLARE @Month INT = MONTH(@CurrentDate)
        DECLARE @Day INT = DAY(@CurrentDate)
        DECLARE @DayOfWeek INT = DATEPART(WEEKDAY, @CurrentDate)

        -- Generate a list of 24 hours for the current date
        DECLARE @Hours TABLE (Hour INT)
        INSERT INTO @Hours VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24)

        -- Insert or update a row for each hour in the @Hours table
        INSERT INTO DimDate (DateKey, aDate, aYear, aQuarter, aMonth, aDay, aDayOfWeek, aHour)
        SELECT 
			   CAST( YEAR(@CurrentDate) * 10000 + MONTH(@CurrentDate) * 100 + DAY(@CurrentDate) AS INT),
			   @CurrentDate AS cdate,
               @Year AS cyear,
               @Quarter AS cquarter,
               @Month AS cmonth,
               @Day AS cday,
               @DayOfWeek AS day_of_week,
               Hour AS chour
        FROM @Hours

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate)
    END
END
GO

EXEC InsertDimDateFromRange '2023-01-01', '2023-04-01';
GO

SELECT * FROM DimDate order by datekey;
GO



-- ======= DimLocation ============


CREATE TABLE DimLocation 
(
	LocationKey			INT PRIMARY KEY NOT NULL,
    IncidentZip			VARCHAR(255)	NULL,
	IncidentAddress		VARCHAR(255)    NULL,
    CityCouncilDistrict INT				NULL,
    PolicePrecinct		VARCHAR(255)	NULL,
    City				VARCHAR(255)	NULL,
    Borough				VARCHAR(50)	    NULL,
    LocationType		VARCHAR(255)	NULL
);
GO


-- ======= DimAgency ============

CREATE TABLE DimAgency 
(
	AgencyKey			INT PRIMARY KEY NOT NULL,
	AgencyName			VARCHAR(255) NULL,
	AgencyDescription	VARCHAR(255) NULL
);
GO

CREATE TABLE DimComplaintType 
(
	ComplaintTypeKey		INT PRIMARY KEY NOT NULL,
	ComplaintType			VARCHAR(255) NULL,
	ComplaintDescription	VARCHAR(MAX) NULL,
	ComplaintLocationType	VARCHAR(255) NULL,
	ComplaintReceivedDate	DATETIME NULL,
);
GO

CREATE TABLE DimStatus 
(
	StatusKey							INT PRIMARY KEY NOT NULL,
	StatusName							VARCHAR(255) NULL,
    StatusResolutionDescription			VARCHAR(MAX) NULL,
	StatusResolutionActionUpdatedDate	DATETIME NULL,
	StatusStartDate						DATE NULL,
    StatusEndDate						DATE NULL,
    StatusDurationDays					INT NULL
);
GO

--Fact Table:

CREATE TABLE FactComplaint 
(
    DateKey INT FOREIGN KEY REFERENCES DimDate(DateKey),
    LocationKey INT FOREIGN KEY REFERENCES DimLocation(LocationKey),
    AgencyKey INT FOREIGN KEY REFERENCES DimAgency(AgencyKey),
    ComplaintTypeKey INT FOREIGN KEY REFERENCES DimComplaintType(ComplaintTypeKey),
    StatusKey INT FOREIGN KEY REFERENCES DimStatus(StatusKey),
    Total_Complaints INT,
    Avg_Resolution_Time_Hours FLOAT,
    Escalation_Rate FLOAT,
    CONSTRAINT PK_FactComplaint PRIMARY KEY (DateKey, LocationKey, AgencyKey, ComplaintTypeKey, StatusKey)
);

-- HOW TO GET THE Total_Complaints PER ROW:
SELECT 
    DimDate.cdate AS Date,
    DimDate.chour AS Hour,
    DimLocation.LocationIncidentZip AS ZipCode,
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
    DimLocation.LocationIncidentZip,
    DimAgency.AgencyName

-- HOW TO GET THE Total_Resolved_Complaints PER ROW:
SELECT 
    DimDate.cdate AS Date,
    DimDate.chour AS Hour,
    DimLocation.LocationIncidentZip AS ZipCode,
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
    DimLocation.LocationIncidentZip,
    DimAgency.AgencyName

-- HOW TO GET THE Total_Unresolved_Complaints PER ROW:
SELECT 
    DimDate.cdate AS Date,
    DimDate.chour AS Hour,
    DimLocation.LocationIncidentZip AS ZipCode,
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
    DimLocation.LocationIncidentZip,
    DimAgency.AgencyName

-- HOW TO GET THE Total_Escalated_Complaints PER ROW:
SELECT 
    DimDate.cdate AS Date,
    DimDate.chour AS Hour,
    DimLocation.LocationIncidentZip AS ZipCode,
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
