CREATE DATABASE NYC_311_REQUESTS_DM;
DROP DATABASE NYC_311_REQUESTS_DM;
USE NYC_311_REQUESTS_DM;
GO


-- ================================ DIMESNIONAL MODEL =======================================

-- ======= DimDate ============
DROP TABLE DIMDATE;
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

CREATE OR ALTER PROCEDURE usp_InsertDimDateRange
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    DECLARE @Date DATE = @EndDate;
    DECLARE @DateKey VARCHAR(15);
    DECLARE @Year INT;
    DECLARE @Quarter INT;
    DECLARE @Month INT;
    DECLARE @Day INT;
    DECLARE @DayOfWeek INT;
    DECLARE @Hour INT;
    
    WHILE @Date >= @StartDate
    BEGIN
        SET @DateKey = CONVERT(VARCHAR, FORMAT(@Date, 'yyyyMMdd'));
        SET @Year = YEAR(@Date);
        SET @Quarter = DATEPART(QUARTER, @Date);
        SET @Month = MONTH(@Date);
        SET @Day = DAY(@Date);
        SET @DayOfWeek = DATEPART(WEEKDAY, @Date);
        
        DECLARE @HourCounter INT = 1;
        WHILE @HourCounter <= 24
        BEGIN
            SET @Hour = @HourCounter - 1;
            SET @DateKey = @DateKey + 1;
            
            INSERT INTO DimDate (DateKey, aDate, aYear, aQuarter, aMonth, aDay, aDayOfWeek, aHour)
            VALUES (CAST( (@DateKey+CAST(@Hour as varchar)) as INT ), @Date, @Year, @Quarter, @Month, @Day, @DayOfWeek, @Hour);
            
            SET @HourCounter = @HourCounter + 1;
        END
        
        SET @Date = DATEADD(DAY, -1, @Date);
    END
END





EXEC usp_InsertDimDateRange '2023-01-01', '2023-04-01';
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
	ComplaintDescription	VARCHAR(MAX) NULL
);
GO

CREATE TABLE DimStatus 
(
	StatusKey							INT PRIMARY KEY NOT NULL,
    StatusBusinessKey                   INT NOT NULL,
	StatusType							VARCHAR(255) NULL,
    StatusResolutionDescription			VARCHAR(MAX) NULL,
	StatusStartDate						DATETIME NULL,
    StatusEndDate						DATETIME NULL, ---> StatusResolutionActionUpdatedDate
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
