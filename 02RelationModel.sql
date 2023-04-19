USE MASTER;
GO



--DROP DATABASE NYC_311_REQUESTS;
--GO

CREATE DATABASE NYC_311;
GO

USE NYC_311;
GO
-- RELATIONAL MODEL
-- =  =  =  =  =  =  =  =  =  =  =  =  =  = AGENCY TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = --

DROP TABLE Agency;
GO
CREATE TABLE Agency
(
    ID                INT PRIMARY KEY IDENTITY,
    AgencyName        VARCHAR(255),
    AgencyDescription VARCHAR(255),
);
GO

CREATE OR ALTER PROCEDURE Agency_Extract AS
BEGIN

    SET NOCOUNT ON;
    INSERT INTO Agency(AgencyName, AgencyDescription)
    SELECT DISTINCT Agency
                  , Agency_Name
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
    WHERE Agency IS NOT NULL;
END
GO
EXEC Agency_Extract;
GO

SELECT *
FROM Agency;
GO
-- =  =  =  =  =  =  =  =  =  =  =  =  =  = COMPLAINT TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = --

DROP TABLE Complaint;
GO
CREATE TABLE Complaint
(
    ID                  INT PRIMARY KEY IDENTITY,
    ComplaintType       VARCHAR(255),
    ComplaintDescriptor VARCHAR(MAX)
);
GO


CREATE OR ALTER PROCEDURE Complaint_Extract AS
BEGIN

    SET NOCOUNT ON;
    INSERT INTO Complaint(ComplaintType, ComplaintDescriptor)
    SELECT DISTINCT Complaint_Type
                  , Descriptor
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
    WHERE Complaint_Type IS NOT NULL;
END
GO
EXEC Complaint_Extract;
GO

SELECT *
FROM Complaint;
GO
-- =  =  =  =  =  =  =  =  =  =  =  =  =  = CITY TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = --

DROP TABLE City;
CREATE TABLE City
(
    ID       INT PRIMARY KEY IDENTITY,
    CityName VARCHAR(255),
);
GO

CREATE OR ALTER PROCEDURE City_Extract AS
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
-- =  =  =  =  =  =  =  =  =  =  =  =  =  = COORDINATES TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = --

CREATE TABLE Coordinates
(
    ID        INT PRIMARY KEY IDENTITY,
    Latitude  FLOAT,
    Longitude FLOAT
);
GO

CREATE OR ALTER PROCEDURE Coordinates_Extract AS
BEGIN

    SET NOCOUNT ON;
    INSERT INTO Coordinates
    SELECT DISTINCT [Latitude]
                  , [Longitude]
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023];
END
GO
EXEC Coordinates_Extract;
GO

SELECT *
FROM Coordinates;
GO
-- =  =  =  =  =  =  =  =  =  =  =  =  =  = STREET ADDRESS TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = --

CREATE TABLE StreetAddress
(
    ID           INT PRIMARY KEY IDENTITY,
    StreetName   VARCHAR(255),
    CrossStreet1 VARCHAR(255),
    CrossStreet2 VARCHAR(255)
);
GO

CREATE OR ALTER PROCEDURE StreetAddress_Extract AS
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
GO
SELECT *
FROM StreetAddress;
-- =  =  =  =  =  =  =  =  =  =  =  =  =  = SERVICE REQUEST LOCATION TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = --

DROP TABLE IncidentLocations;
GO
CREATE TABLE IncidentLocations
(
    ID                  INT PRIMARY KEY IDENTITY,
    IncidentZip         VARCHAR(255),
    IncidentAddress     VARCHAR(255),
    AddressType         VARCHAR(255),
    Landmark            VARCHAR(255),
    StreetAddressID     INT REFERENCES StreetAddress (ID),
    CoordinatesID       INT FOREIGN KEY REFERENCES Coordinates (ID),
);
GO

CREATE OR ALTER PROCEDURE IncidentLocations_Extract AS
BEGIN

    SET NOCOUNT ON;
    INSERT INTO IncidentLocations (IncidentZip, IncidentAddress, 
                                   AddressType, Landmark, StreetAddressID, CoordinatesID)
    SELECT [Incident_Zip]
         , [Incident_Address]
         , [Address_Type]
         , [Landmark]
         , sa.ID
         , Coordinates.ID
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] nyc
             LEFT JOIN StreetAddress sa
                       ON sa.StreetName = nyc.Street_Name AND sa.CrossStreet1 = nyc.Cross_Street_1 AND
                          sa.CrossStreet2 = nyc.Cross_Street_2
             LEFT JOIN Coordinates
                       ON Coordinates.Latitude = nyc.[Latitude] AND Coordinates.Longitude = nyc.[Longitude]
END
GO

CREATE NONCLUSTERED INDEX idx_IncidentLocations
    ON IncidentLocations (IncidentZip, IncidentAddress);
GO

EXEC IncidentLocations_Extract;
GO

SELECT *
FROM dbo.IncidentLocations;
GO
--- =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = Borough Boundary =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = ----

DROP TABLE BoroughBoundary;

CREATE TABLE BoroughBoundary
(

    BoroughName  VARCHAR(50),
    BoroughValue INT PRIMARY KEY,
);
GO

CREATE OR ALTER PROCEDURE BoroughBoundary_Extraction
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO BoroughBoundary (BoroughName, BoroughValue) VALUES ('Bronx', 2);
    INSERT INTO BoroughBoundary (BoroughName, BoroughValue) VALUES ('Brooklyn', 3);
    INSERT INTO BoroughBoundary (BoroughName, BoroughValue) VALUES ('Manhattan', 1);
    INSERT INTO BoroughBoundary (BoroughName, BoroughValue) VALUES ('Queens', 4);
    INSERT INTO BoroughBoundary (BoroughName, BoroughValue) VALUES ('Staten Island', 5);

END
GO


EXEC BoroughBoundary_Extraction;
Go

SELECT *
FROM BoroughBoundary;
Go
--- =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = City Council Boundary =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = ----

CREATE TABLE CityCouncilBoundary
(

    District            INT PRIMARY KEY,
    CouncilGovernerName VARCHAR(255),
    BoroughID           INT FOREIGN KEY (BoroughID) REFERENCES BoroughBoundary (BoroughValue),
);
GO

CREATE OR ALTER PROCEDURE CityCouncilBoundary_Extraction AS
BEGIN
    INSERT INTO CityCouncilBoundary (District, CouncilGovernerName, BoroughID)
    SELECT [DISTRICT], [NAME], bb.BoroughValue
    FROM [NYC_311_REQUESTS].[dbo].[Council_Members] nyc
             INNER JOIN BoroughBoundary bb ON nyc.[BOROUGH] = bb.BoroughName
END
GO

EXEC CityCouncilBoundary_Extraction;

SELECT *
FROM CityCouncilBoundary;

--- =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = Police Precint Boundary =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = ----
DROP TABLE PolicePrecinct;
CREATE TABLE PolicePrecinct
(
    PrecinctNumber  INT PRIMARY KEY,
    PrecinctName    VARCHAR(255),
    PhoneNumber     VARCHAR(20),
    PrecinctAddress VARCHAR(255),
    BoroughID       INT FOREIGN KEY (BoroughID) REFERENCES BoroughBoundary (BoroughValue)
);
GO

CREATE OR ALTER PROCEDURE PolicePrecinct_Extraction AS
BEGIN
    INSERT INTO PolicePrecinct (PrecinctNumber, PrecinctName, PhoneNumber, PrecinctAddress, BoroughID)
    SELECT [Precinct_Number]
         , [Name]
         , [Phone_number]
         , [Address]
         , bb.BoroughValue
    FROM [NYC_311_REQUESTS].[dbo].[Police_Precint] nyc
             INNER JOIN BoroughBoundary bb ON nyc.[Borough] = bb.BoroughName
END
GO

EXEC PolicePrecinct_Extraction;

SELECT *
FROM PolicePrecinct;

--- =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = Resolution TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = ----

DROP TABLE Resolution;
CREATE TABLE Resolution
(
    ID                          INT PRIMARY KEY IDENTITY,
    ServiceKeyID                INT UNIQUE,
    ResolutionDescription       VARCHAR(MAX),
    ResolutionActionUpdatedDate DATETIME,
    ResolutionStatus            VARCHAR(255),
);
GO

CREATE OR ALTER PROCEDURE Resolution_Extract AS
BEGIN
    INSERT INTO Resolution (ServiceKeyID, ResolutionDescription, ResolutionActionUpdatedDate, ResolutionStatus)
    SELECT Unique_Key
         , [Resolution_Description]
         , Resolution_Action_Updated_Date
         , [Status]
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] r
END
GO
EXEC Resolution_Extract;

SELECT *
FROM Resolution;
--- =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = Service Request =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = ----

DROP TABLE ServiceRequest;

CREATE TABLE ServiceRequest
(
    UniqueKey             INT PRIMARY KEY IDENTITY,
    ServiceKeyID          INT,
    CreatedDate           DATETIME,
    ClosedDate            DATETIME,
    DueDate               DATETIME,
    ResolutionID          INT FOREIGN KEY REFERENCES Resolution (ID),
    AgencyID              INT FOREIGN KEY REFERENCES Agency (ID),
    LocationID            INT FOREIGN KEY REFERENCES IncidentLocations (ID),
    CityID                INT FOREIGN KEY REFERENCES City (ID),
    CityCouncilDistrictID INT FOREIGN KEY REFERENCES CityCouncilBoundary (District),
    BoroughBoundaryID     INT FOREIGN KEY REFERENCES BoroughBoundary (BoroughValue),
    PolicePrecinctID      INT FOREIGN KEY REFERENCES PolicePrecinct (PrecinctNumber),
);
GO


CREATE OR ALTER PROCEDURE ServiceRequests_Extract AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO ServiceRequest (ServiceKeyID, CreatedDate, ClosedDate, DueDate, ResolutionID, AgencyID, LocationID,
                                BoroughBoundaryID, CityCouncilDistrictID, PolicePrecinctID)
    SELECT r.Unique_Key
         , r.Created_Date
         , r.Closed_Date
         , r.Due_Date
         , rs.ID
         , a.ID              AS AgencyID
         , l.ID              AS LocationID
         , bb.BoroughValue   AS BoroughBoundaryID
         , cc.District       AS CityCouncilDistrictID
         , pp.PrecinctNumber AS PolicePrecinctID
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] r
             LEFT JOIN Resolution rs
                       ON r.Unique_Key = rs.ServiceKeyID
             LEFT JOIN Agency a
                       ON r.Agency = a.AgencyName
             LEFT JOIN IncidentLocations l
                       ON r.Incident_Zip = l.IncidentZip AND r.Incident_Address = l.IncidentAddress
             LEFT JOIN City
                       ON City.CityName = r.[City]
             LEFT JOIN BoroughBoundary bb
                       ON r.Borough_Boundaries = bb.BoroughValue
             LEFT JOIN CityCouncilBoundary cc
                       ON r.City_Council_Districts = cc.District
             LEFT JOIN PolicePrecinct pp
                       ON r.Police_Precincts = pp.PrecinctNumber
END
GO

EXEC ServiceRequests_Extract;
GO

USE NYC_311;

SELECT *
FROM ServiceRequest;
GO
--- =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = ServiceRequest_Complaint Junction TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = ---
DROP TABLE ServiceRequest_Complaint;
GO
CREATE TABLE ServiceRequest_Complaint
(
    ServiceRequestID INT FOREIGN KEY REFERENCES ServiceRequest (UniqueKey),
    ComplaintID      INT FOREIGN KEY REFERENCES Complaint (ID)
);
GO


CREATE OR ALTER PROCEDURE ServiceRequest_Complaint_Insert AS
BEGIN

    SET NOCOUNT ON;
    INSERT INTO ServiceRequest_Complaint (ServiceRequestID, ComplaintID)
    SELECT s.UniqueKey
         , c.ID
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] r
             INNER JOIN ServiceRequest s
                        ON r.Unique_Key = s.ServiceKeyID
             INNER JOIN Complaint c
                        ON r.Complaint_Type = c.ComplaintType
END
GO

CREATE NONCLUSTERED INDEX IDX_ServiceRequest_ServiceKeyID
    ON ServiceRequest (ServiceKeyID);
GO


CREATE NONCLUSTERED INDEX IDX_Complaint
    ON Complaint (ComplaintType);
GO

EXECUTE ServiceRequest_Complaint_Insert;
GO

SELECT *
FROM ServiceRequest_Complaint;