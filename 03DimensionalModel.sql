CREATE DATABASE NYC_311_REQUESTS_DM;
DROP DATABASE NYC_311_REQUESTS_DM;
USE NYC_311_REQUESTS_DM;
GO

DROP TABLE DimDate;
-- Dimensional Modelling
CREATE TABLE DimDate (
   DateKey INT PRIMARY KEY,
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
        DECLARE @DateKey INT = CAST(FORMAT(@CurrentDate, 'yyyyMMdd') AS INT)
        DECLARE @Year INT = YEAR(@CurrentDate)
        DECLARE @Quarter INT = DATEPART(QUARTER, @CurrentDate)
        DECLARE @Month INT = MONTH(@CurrentDate)
        DECLARE @Day INT = DAY(@CurrentDate)
        DECLARE @DayOfWeek INT = DATEPART(WEEKDAY, @CurrentDate)

        -- Loop through hours 1-24 and insert or update a row for each hour
        DECLARE @Hour INT = 1
        WHILE @Hour <= 24
        BEGIN
            MERGE INTO DimDate WITH (HOLDLOCK) AS target
            USING (
                SELECT @DateKey + @Hour AS DateKey,
                       CAST(@CurrentDate AS DATETIME) AS cdate,
                       @Year AS cyear,
                       @Quarter AS cquarter,
                       @Month AS cmonth,
                       @Day AS cday,
                       @DayOfWeek AS day_of_week,
                       @Hour AS chour
            ) AS source
            ON (target.DateKey = source.DateKey)
            WHEN MATCHED THEN
                UPDATE SET
                    cdate = source.cdate,
                    cyear = source.cyear,
                    cquarter = source.cquarter,
                    cmonth = source.cmonth,
                    cday = source.cday,
                    day_of_week = source.day_of_week,
                    chour = source.chour
            WHEN NOT MATCHED THEN
                INSERT (DateKey, cdate, cyear, cquarter, cmonth, cday, day_of_week, chour)
                VALUES (source.DateKey, source.cdate, source.cyear, source.cquarter, source.cmonth, source.cday, source.day_of_week, source.chour);

            SET @Hour = @Hour + 1
        END

        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate)
    END
END



EXEC InsertDimDateFromRange '2023-01-01', '2023-04-01';
GO

SELECT COUNT(*) FROM DimDate;
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