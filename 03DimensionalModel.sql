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

DROP TABLE DIMTIME;
CREATE TABLE DimTime  ---SCD TYPE 0
(
   TimeKey     INT PRIMARY KEY,
   aTime       TIME(0) NOT NULL,
   anHour      INT NOT NULL,
   aMinute     INT NOT NULL,
);
GO


-- ======= DimLocation ============


CREATE TABLE DimComplaintLocation --- Type SCD 0
(
	LocationKey			INT PRIMARY KEY NOT NULL,
    Zip			        VARCHAR(255)	NULL,
	LocationAddress		VARCHAR(255)    NULL,
    CityCouncilDistrict INT				NULL,
    PolicePrecinct		VARCHAR(255)	NULL,
    City				VARCHAR(255)	NULL,
    Borough				VARCHAR(50)	    NULL,
    LocationType		VARCHAR(255)	NULL
);
GO


-- ======= DimAgency ============

CREATE TABLE DimAgency --- Type SCD 1
(
	AgencyKey			INT PRIMARY KEY NOT NULL,
	AgencyName			VARCHAR(255) NULL,
	AgencyDescription	VARCHAR(255) NULL,
    UpdateDate              DATETIME NOT NULL
);
GO

CREATE TABLE DimComplaintType --- Type SCD 1
(
	ComplaintTypeKey		INT PRIMARY KEY NOT NULL,
	ComplaintType			VARCHAR(255) NULL,
	ComplaintDescription	VARCHAR(MAX) NULL,
    UpdateDate              DATETIME NOT NULL
);
GO

CREATE TABLE DimStatus --- Type SCD 2
(
	StatusKey							INT PRIMARY KEY NOT NULL,
    StatusID                            INT NOT NULL,
	StatusType							VARCHAR(255) NULL,
    StatusResolutionDescription			VARCHAR(MAX) NULL,
    StatusDurationDays					INT NULL,
	StatusStarted						DATETIME NULL,
    StatusUpdatedDate                   DATETIME NULL,
    StatusEnded						    DATETIME NULL, ---> StatusResolutionActionUpdatedDate
);
GO

--Fact Table:

CREATE TABLE FactComplaint 
(
    DateKey INT FOREIGN KEY REFERENCES DimDate(DateKey),
    LocationKey INT FOREIGN KEY REFERENCES DimComplaintLocation(LocationKey),
    AgencyKey INT FOREIGN KEY REFERENCES DimAgency(AgencyKey),
    ComplaintTypeKey INT FOREIGN KEY REFERENCES DimComplaintType(ComplaintTypeKey),
    StatusKey INT FOREIGN KEY REFERENCES DimStatus(StatusKey),
    Total_Complaints INT,
    Total_Resolved_Complaints INT,
    Total_Unresolved_Complaints INT,
    Total_Escalated_Complaints INT,
    Total_Reassigned_Complaints INT,
    Avg_Resolution_Time_Hours FLOAT,
    Escalation_Rate FLOAT,
    CONSTRAINT PK_FactComplaint PRIMARY KEY (DateKey, LocationKey, AgencyKey, ComplaintTypeKey, StatusKey)
);

GO


----================ PROCEDURE TO INSERT DATE ==================----
CREATE OR ALTER PROCEDURE DimDateInsertion
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Create a temporary table to hold the date range
    DECLARE @DateTable TABLE (aDate DATE);
    
    -- Insert the range of dates into the temporary table
    WHILE @StartDate <= @EndDate
    BEGIN
        INSERT INTO @DateTable (aDate) VALUES (@StartDate);
        SET @StartDate = DATEADD(day, 1, @StartDate);
    END
    
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
END


EXEC DimDateInsertion '2023-01-01', '2023-04-01';
GO

SELECT * FROM DimDate order by datekey;
GO

----================ PROCEDURE TO INSERT TIME ==================----
CREATE OR ALTER PROCEDURE PopulateDimTimeForDay
AS
BEGIN
    DECLARE @date DATE = '2023-04-19';
    DECLARE @hour INT = 0;
    DECLARE @minute INT = 0;

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
END


EXEC PopulateDimTimeForDay;
GO

SELECT * FROM DimTime;























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
