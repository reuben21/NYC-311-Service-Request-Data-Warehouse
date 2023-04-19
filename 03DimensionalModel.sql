CREATE DATABASE NYC_311_REQUESTS_DM;
DROP DATABASE NYC_311_REQUESTS_DM;
USE NYC_311_REQUESTS_DM;
GO

-- DROP TABLE DimDate;
-- Dimensional Modelling
CREATE TABLE DimDate (
   DateKey INT PRIMARY KEY IDENTITY(1,1),
   cdate DATE,
   cyear INT,
   cquarter INT,
   cmonth INT,
   cday INT,
   day_of_week INT,
   chour INT
);
GO

CREATE OR ALTER PROCEDURE InsertDimDateFromRange
    @StartDate DATE,
    @EndDate DATE
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
        INSERT INTO DimDate (cdate, cyear, cquarter, cmonth, cday, day_of_week, chour)
        SELECT @CurrentDate AS cdate,
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

EXEC InsertDimDateFromRange '2023-01-01', '2023-04-01';
GO

SELECT * FROM DimDate;
GO

CREATE TABLE DimLocation (
	LocationKey INT PRIMARY KEY,
    LocationIncidentZip VARCHAR(255),
	LocationIncidentAddress VARCHAR(255),
    Landmark VARCHAR(255),
    LocationType VARCHAR(255)
);
GO

CREATE TABLE DimAgency (
	AgencyKey INT PRIMARY KEY,
	AgencyName VARCHAR(255),
	AgencyDescription VARCHAR(MAX)
);
GO

CREATE TABLE DimComplaintType (
	ComplaintTypeKey INT PRIMARY KEY,
	ComplaintType VARCHAR(255),
	ComplaintDescription VARCHAR(MAX),
	ComplaintLocationType VARCHAR(255),
	ComplaintReceivedDate DATE,
);
GO

CREATE TABLE DimStatus (
	StatusKey INT PRIMARY KEY,
	StatusName VARCHAR(255),
    StatusResolutionDescription VARCHAR(MAX),
	StatusResolutionActionUpdatedDate DATETIME,
	StatusStartDate DATE,
    StatusEndDate DATE,
    StatusDurationDays INT
);
GO

--Fact Table:

CREATE TABLE FactComplaint (
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
    COUNT(CASE WHEN DimStatus.StatusName = 'Resolved' THEN 1 ELSE NULL END) AS Total_Resolved_Complaints
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
    COUNT(CASE WHEN DimStatus.StatusName NOT IN ('Resolved', 'Closed') THEN 1 ELSE NULL END) AS Total_Unresolved_Complaints
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


/*
	Unnecessary Dimensions:
*/
-- PolicePrecinctKey INT FOREIGN KEY (PolicePrecinctKey) REFERENCES DimPolicePrecinct(PolicePrecinctKey),
	-- CityCouncilDistrictKey INT FOREIGN KEY (CityCouncilDistrictKey) REFERENCES DimCityCouncilDistrict(CityCouncilDistrictKey),
	--  BoroughBoundaryKey INT FOREIGN KEY (BoroughBoundaryKey) REFERENCES DimBoroughBoundary(BoroughBoundaryKey)
	
	
--    Total_Resolved_Complaints INT,
--     Total_Unresolved_Complaints INT,
--     Total_Escalated_Complaints INT,
--     Total_Reassigned_Complaints INT,