USE MASTER;
GO
-- Indexing & Modifying Columns
--ALTER TABLE [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
--ALTER COLUMN [Agency] VARCHAR(10);

--ALTER TABLE [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
--ALTER COLUMN [Incident_Zip] VARCHAR(10);

--ALTER TABLE [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
--ALTER COLUMN [Incident_Address] VARCHAR(50);

--CREATE NONCLUSTERED INDEX IX_Agency
--ON [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] (Agency);

--CREATE NONCLUSTERED INDEX IX_IncidentZip
--ON [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] (Incident_Zip)

--CREATE NONCLUSTERED INDEX IX_IncidentAddress
--ON [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] (Incident_Address);


--DROP DATABASE NYC_311_REQUESTS;
--GO

CREATE DATABASE NYC_311;
GO


USE NYC_311;
GO

-- RELATIONAL MODEL
--==============AGENCY TABLE==============--
DROP TABLE Agency;
GO


CREATE TABLE Agency
(
    ID                INT PRIMARY KEY IDENTITY,
    AgencyName        VARCHAR(255),
    AgencyDescription VARCHAR(255),
);
GO

CREATE OR ALTER PROCEDURE Agency_Extract
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO Agency(AgencyName, AgencyDescription)
    SELECT DISTINCT Agency,
                    Agency_Name
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
    WHERE Agency IS NOT NULL;
END
GO

EXEC Agency_Extract;
GO

SELECT *
FROM Agency;
GO
--==============COMPLAINT TABLE==============--
DROP TABLE Complaint;
GO

CREATE TABLE Complaint
(
    ID                  INT PRIMARY KEY IDENTITY,
    ComplaintType       VARCHAR(255),
    ComplaintDescriptor VARCHAR(MAX)
);
GO

CREATE OR ALTER PROCEDURE Complaint_Extract
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO Complaint(ComplaintType, ComplaintDescriptor)
    SELECT DISTINCT Complaint_Type,
                    Descriptor
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
    WHERE Complaint_Type IS NOT NULL;
END
GO

EXEC Complaint_Extract;
GO

SELECT *
from Complaint;
GO

--==============CITY TABLE==============--
DROP TABLE City;
CREATE TABLE City
(
    ID   INT PRIMARY KEY IDENTITY,
    CityName VARCHAR(255),
);
GO

CREATE OR ALTER PROCEDURE City_Extract
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO City (CityName)
    SELECT DISTINCT [City]
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023];
END
GO

EXEC City_Extract;
GO

SELECT *
FROM CITY;
GO

--==============COORDINATES TABLE==============--
CREATE TABLE Coordinates
(
    ID        INT PRIMARY KEY IDENTITY,
    Latitude  FLOAT,
    Longitude FLOAT
);
GO

CREATE OR ALTER PROCEDURE Coordinates_Extract
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO Coordinates
    SELECT DISTINCT [Latitude],
                    [Longitude]
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023];
END
GO

EXEC Coordinates_Extract;
GO


SELECT *
FROM Coordinates;
GO

--==============STREET ADDRESS TABLE==============--

CREATE TABLE StreetAddress
(
    ID           INT PRIMARY KEY IDENTITY,
    StreetName   VARCHAR(255),
    CrossStreet1 VARCHAR(255),
    CrossStreet2 VARCHAR(255)
);
GO

CREATE OR ALTER PROCEDURE StreetAddress_Extract
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO StreetAddress (StreetName, CrossStreet1, CrossStreet2)
    SELECT DISTINCT [Street_Name]
                  , [Cross_Street_1]
                  , [Cross_Street_2]
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]

END
GO

EXEC StreetAddress_Extract;

SELECT *
FROM StreetAddress;


--==============SERVICE REQUEST LOCATION TABLE==============--
DROP TABLE ServiceRequestLocation;
GO

CREATE TABLE ServiceRequestLocation
(
    ID                  INT PRIMARY KEY IDENTITY,
    IncidentZip         VARCHAR(255),
    IncidentAddress     VARCHAR(255),
    IntersectionStreet1 VARCHAR(255),
    IntersectionStreet2 VARCHAR(255),
    AddressType         VARCHAR(255),
    Landmark            VARCHAR(255),
    StreetAddressID     INT REFERENCES StreetAddress (ID),
    CityID              INT FOREIGN KEY REFERENCES City (ID) ,
    CoordinatesID       INT FOREIGN KEY REFERENCES Coordinates (ID),
);
GO

CREATE OR ALTER PROCEDURE ServiceRequestLocation_Extract
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ServiceRequestLocation (IncidentZip,
                                        IncidentAddress,
                                        IntersectionStreet1,
                                        IntersectionStreet2,
                                        AddressType,
                                        Landmark,
                                        StreetAddressID,
                                        CityID,
                                        CoordinatesID)
    SELECT [Incident_Zip],
           [Incident_Address],
           [Intersection_Street_1],
           [Intersection_Street_2],
           [Address_Type],
           [Landmark],
           sa.ID,
           City.ID,
           Coordinates.ID

    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] nyc
             INNER JOIN StreetAddress sa ON sa.StreetName = nyc.Street_Name
			AND sa.CrossStreet1 = nyc.Cross_Street_1
			AND sa.CrossStreet2 = nyc.Cross_Street_2
             INNER JOIN City ON City.CityName = nyc.[City]
             INNER JOIN Coordinates ON Coordinates.Latitude = nyc.[Latitude]
        AND Coordinates.Longitude = nyc.[Longitude]
END
GO


EXEC ServiceRequestLocation_Extract;
GO

SELECT *
FROM dbo.ServiceRequestLocation;
GO


---====================Geography Boundary=================----
CREATE TABLE GeographicBoundary
(
    ID             INT PRIMARY KEY IDENTITY,
    GeographicType VARCHAR(50),
    Number         BIGINT
);
GO

CREATE OR ALTER PROCEDURE GeographicData_Extraction
AS
BEGIN
    INSERT INTO GeographicBoundary (GeographicType, Number)
    SELECT DISTINCT 'Community District', Community_Districts
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
    UNION ALL
    SELECT DISTINCT 'Borough Boundary', Borough_Boundaries
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
    UNION ALL
    SELECT DISTINCT 'City Council District', City_Council_Districts
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
    UNION ALL
    SELECT DISTINCT 'Police Precinct', Police_Precincts
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
END
GO

EXEC GeographicData_Extraction;
GO

SELECT *
FROM GeographicBoundary;
GO
---====================Resolution Table =================----
DROP TABLE Resolution;

CREATE TABLE Resolution
(
    ID                   INT PRIMARY KEY IDENTITY,
	ServiceKeyID                INT UNIQUE,
    ResolutionDescription        VARCHAR(MAX),
    ResolutionActionUpdatedDate DATETIME,
    ResolutionStatus        VARCHAR(255),
);
GO


CREATE OR ALTER PROCEDURE Resolution_Extract
AS
BEGIN
    INSERT INTO Resolution (	ServiceKeyID,
                                ResolutionDescription,
                                ResolutionActionUpdatedDate,
                                ResolutionStatus
								)
    SELECT Unique_Key,
           [Resolution_Description],
            Resolution_Action_Updated_Date,
           [Status]
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] r
            
         
END
GO

EXEC Resolution_Extract;

SELECT * FROM Resolution;

---====================Service Request=================----
DROP TABLE ServiceRequest;

CREATE TABLE ServiceRequest
(
    UniqueKey                   INT PRIMARY KEY IDENTITY,
    ServiceKeyID                INT UNIQUE,
    CreatedDate                 DATETIME,
    ClosedDate                  DATETIME,
    DueDate                     DATETIME,
    ResolutionID				INT FOREIGN KEY (ResolutionID) REFERENCES Resolution (ID),
    AgencyID                    INT FOREIGN KEY (AgencyID) REFERENCES Agency (ID),
    LocationID                  INT FOREIGN KEY (LocationID) REFERENCES ServiceRequestLocation (ID),
    CommunityDistrictID         INT FOREIGN KEY REFERENCES GeographicBoundary (ID),
    BoroughBoundaryID           INT FOREIGN KEY REFERENCES GeographicBoundary (ID),
    CityCouncilDistrictID       INT FOREIGN KEY REFERENCES GeographicBoundary (ID),
    PolicePrecinctID            INT FOREIGN KEY REFERENCES GeographicBoundary (ID),
);
GO


CREATE OR ALTER PROCEDURE ServiceRequests_Extract
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO ServiceRequest (ServiceKeyID,
                                CreatedDate,
                                ClosedDate,
                                DueDate,
                                ResolutionID,
                                AgencyID,
                                LocationID,
                                CommunityDistrictID,
                                BoroughBoundaryID,
                                CityCouncilDistrictID,
                                PolicePrecinctID)
    SELECT r.Unique_Key,
           r.Created_Date,
           r.Closed_Date,
           r.Due_Date,
           rs.ID,
           a.ID AS AgencyID,
           l.ID AS LocationID,
           g1.ID AS CommunityDistrictID,
           g2.ID AS BoroughBoundaryID,
           g3.ID AS CityCouncilDistrictID,
           g4.ID AS PolicePrecinctID
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] r
    LEFT JOIN Resolution rs
        ON r.Unique_Key = rs.ServiceKeyID
    LEFT JOIN Agency a
        ON r.Agency = a.AgencyName
    LEFT JOIN (
        SELECT IncidentZip, IncidentAddress, ID
        FROM ServiceRequestLocation
    ) l
        ON r.Incident_Zip = l.IncidentZip
        AND r.Incident_Address = l.IncidentAddress
    LEFT JOIN (
        SELECT Number, ID
        FROM GeographicBoundary
        WHERE GeographicType = 'Community District'
    ) g1
        ON r.Community_Districts = g1.Number
    LEFT JOIN (
        SELECT Number, ID
        FROM GeographicBoundary
        WHERE GeographicType = 'Borough Boundary'
    ) g2
        ON r.Borough_Boundaries = g2.Number
    LEFT JOIN (
        SELECT Number, ID
        FROM GeographicBoundary
        WHERE GeographicType = 'City Council District'
    ) g3
        ON r.City_Council_Districts = g3.Number
    LEFT JOIN (
        SELECT Number, ID
        FROM GeographicBoundary
        WHERE GeographicType = 'Police Precinct'
    ) g4
        ON r.Police_Precincts = g4.Number;
END
GO


EXEC ServiceRequests_Extract;
GO


SELECT *
FROM ServiceRequest;
GO

---================== ServiceRequest_Complaint Junction TABLE==============---

CREATE TABLE ServiceRequest_Complaint
(
    ServiceRequestID INT,
    ComplaintID      INT,
    FOREIGN KEY (ServiceRequestID) REFERENCES ServiceRequest (UniqueKey),
    FOREIGN KEY (ComplaintID) REFERENCES Complaint (ID)
);
GO

DROP TABLE ServiceRequest_Complaint;
GO

CREATE OR ALTER PROCEDURE ServiceRequest_Complaint_Insert
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ServiceRequest_Complaint (ServiceRequestID, ComplaintID)
    SELECT s.UniqueKey, c.ID
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] r
             INNER JOIN ServiceRequest s ON r.Unique_Key = s.ServiceKeyID
             INNER JOIN Complaint c ON r.Complaint_Type = c.ComplaintType
END
GO

EXECUTE ServiceRequest_Complaint_Insert;
GO

