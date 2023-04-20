USE MASTER;
GO

CREATE DATABASE NYC_311;
GO

USE NYC_311;
GO

-- RAW TABLE (JAN 2023 - MARCH 2023)
DROP TABLE IF EXISTS [dbo].[311_JAN_2023_TO_MAR_2023]
GO
CREATE TABLE [dbo].[311_JAN_2023_TO_MAR_2023](
	[Unique_Key] [varchar](max) NOT NULL,
	[Created_Date] [varchar](max) NULL,
	[Closed_Date] [varchar](max) NULL,
	[Agency] [varchar](max) NULL,
	[Agency_Name] [varchar](max) NULL,
	[Complaint_Type] [varchar](max) NULL,
	[Descriptor] [varchar](max) NULL,
	[Location_Type] [varchar](max) NULL,
	[Incident_Zip] [varchar](max) NULL,
	[Incident_Address] [varchar](max) NULL,
	[Street_Name] [varchar](max) NULL,
	[Cross_Street_1] [varchar](max) NULL,
	[Cross_Street_2] [varchar](max) NULL,
	[Intersection_Street_1] [varchar](max) NULL,
	[Intersection_Street_2] [varchar](max) NULL,
	[Address_Type] [varchar](max) NULL,
	[City] [varchar](max) NULL,
	[Landmark] [varchar](max) NULL,
	[Facility_Type] [varchar](max) NULL,
	[Status] [varchar](max) NULL,
	[Due_Date] [varchar](max) NULL,
	[Resolution_Description] [varchar](max) NULL,
	[Resolution_Action_Updated_Date] [varchar](max) NULL,
	[Community_Board] [varchar](max) NULL,
	[BBL] [varchar](max) NULL,
	[Borough] [varchar](max) NULL,
	[X_Coordinate_State_Plane] [varchar](max) NULL,
	[Y_Coordinate_State_Plane] [varchar](max) NULL,
	[Open_Data_Channel_Type] [varchar](max) NULL,
	[Park_Facility_Name] [varchar](max) NULL,
	[Park_Borough] [varchar](max) NULL,
	[Vehicle_Type] [varchar](max) NULL,
	[Taxi_Company_Borough] [varchar](max) NULL,
	[Taxi_Pick_Up_Location] [varchar](max) NULL,
	[Bridge_Highway_Name] [varchar](max) NULL,
	[Bridge_Highway_Direction] [varchar](max) NULL,
	[Road_Ramp] [varchar](max) NULL,
	[Bridge_Highway_Segment] [varchar](max) NULL,
	[Latitude] [varchar](max) NULL,
	[Longitude] [varchar](max) NULL,
	[LocationName] [varchar](max) NULL,
	[Zip_Codes] [varchar](max) NULL,
	[Community_Districts] [varchar](max) NULL,
	[Borough_Boundaries] [varchar](max) NULL,
	[City_Council_Districts] [varchar](max) NULL,
	[Police_Precincts] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


--======================= RELATIONAL MODEL =====================================


-- =  =  =  =  =  =  =  =  =  =  =  =  =  = AGENCY TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = --

DROP TABLE IF EXISTS Agency;
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
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;

    INSERT INTO Agency(AgencyName, AgencyDescription)
    SELECT DISTINCT Agency
                  , Agency_Name
    FROM [NYC_311].[dbo].[311_JAN_2023_TO_MAR_2023]
    WHERE Agency IS NOT NULL;

	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;

END
GO

EXEC Agency_Extract;
GO

SELECT *
FROM Agency;
GO
-- =  =  =  =  =  =  =  =  =  =  =  =  =  = COMPLAINT TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = --

DROP TABLE IF EXISTS Complaint;
GO
CREATE TABLE Complaint
(
    ID                  INT PRIMARY KEY IDENTITY,
    ComplaintType       VARCHAR(255),
    ComplaintDescriptor VARCHAR(500)
);
GO


CREATE OR ALTER PROCEDURE Complaint_Extract AS
BEGIN

    SET NOCOUNT ON;
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;

    INSERT INTO Complaint(ComplaintType, ComplaintDescriptor)
    SELECT DISTINCT Complaint_Type
                  , Descriptor
    FROM [NYC_311].[dbo].[311_JAN_2023_TO_MAR_2023]
    WHERE Complaint_Type IS NOT NULL;

	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC Complaint_Extract;
GO

SELECT *
FROM Complaint;
GO
-- =  =  =  =  =  =  =  =  =  =  =  =  =  = CITY TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = --

DROP TABLE IF EXISTS City;
GO
CREATE TABLE City
(
    ID       INT PRIMARY KEY IDENTITY,
    CityName VARCHAR(255),
);
GO

CREATE OR ALTER PROCEDURE City_Extract AS
BEGIN

    SET NOCOUNT ON;
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;
    INSERT INTO City (CityName)
    SELECT DISTINCT [City]
    FROM [NYC_311].[dbo].[311_JAN_2023_TO_MAR_2023];

	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC City_Extract;
GO

SELECT *
FROM CITY;
GO
-- =  =  =  =  =  =  =  =  =  =  =  =  =  = COORDINATES TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = --
DROP TABLE IF EXISTS Coordinates
GO
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
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;

    INSERT INTO Coordinates
    SELECT DISTINCT [Latitude]
                  , [Longitude]
    FROM [NYC_311].[dbo].[311_JAN_2023_TO_MAR_2023];
	
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC Coordinates_Extract;
GO

SELECT *
FROM Coordinates;
GO
-- =  =  =  =  =  =  =  =  =  =  =  =  =  = STREET ADDRESS TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = --
DROP TABLE IF EXISTS StreetAddress
GO
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
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;

    INSERT INTO StreetAddress (StreetName, CrossStreet1, CrossStreet2)
    SELECT DISTINCT [Street_Name]
                  , [Cross_Street_1]
                  , [Cross_Street_2]
    FROM [NYC_311].[dbo].[311_JAN_2023_TO_MAR_2023]
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC StreetAddress_Extract;
GO

SELECT *
FROM StreetAddress;
GO
-- =  =  =  =  =  =  =  =  =  =  =  =  =  = SERVICE REQUEST LOCATION TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  = --

DROP TABLE  IF EXISTS IncidentLocations;
GO
CREATE TABLE IncidentLocations
(
    ID                  INT PRIMARY KEY IDENTITY,
    IncidentZip         VARCHAR(255),
    IncidentAddress     VARCHAR(255),
    IntersectionStreet1 VARCHAR(255),
    IntersectionStreet2 VARCHAR(255),
    AddressType         VARCHAR(255),
    Landmark            VARCHAR(255),
    StreetAddressID     INT REFERENCES StreetAddress (ID),
    CoordinatesID       INT FOREIGN KEY REFERENCES Coordinates (ID),
);
GO

CREATE OR ALTER PROCEDURE IncidentLocations_Extract AS
BEGIN

    SET NOCOUNT ON;
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;

    INSERT INTO IncidentLocations (IncidentZip, IncidentAddress, IntersectionStreet1, IntersectionStreet2,
                                   AddressType, Landmark, StreetAddressID, CoordinatesID)
    SELECT [Incident_Zip]
         , [Incident_Address]
         , [Intersection_Street_1]
         , [Intersection_Street_2]
         , [Address_Type]
         , [Landmark]
         , sa.ID
         , Coordinates.ID
    FROM [NYC_311].[dbo].[311_JAN_2023_TO_MAR_2023] nyc
             LEFT JOIN StreetAddress sa
                       ON sa.StreetName = nyc.Street_Name AND sa.CrossStreet1 = nyc.Cross_Street_1 AND
                          sa.CrossStreet2 = nyc.Cross_Street_2
             LEFT JOIN Coordinates
                       ON Coordinates.Latitude = nyc.[Latitude] AND Coordinates.Longitude = nyc.[Longitude]

	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
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

DROP TABLE IF EXISTS BoroughBoundary;
GO

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
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;

    INSERT INTO BoroughBoundary (BoroughName, BoroughValue) VALUES ('Bronx', 2);
    INSERT INTO BoroughBoundary (BoroughName, BoroughValue) VALUES ('Brooklyn', 3);
    INSERT INTO BoroughBoundary (BoroughName, BoroughValue) VALUES ('Manhattan', 1);
    INSERT INTO BoroughBoundary (BoroughName, BoroughValue) VALUES ('Queens', 4);
    INSERT INTO BoroughBoundary (BoroughName, BoroughValue) VALUES ('Staten Island', 5);
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO


EXEC BoroughBoundary_Extraction;
Go

SELECT *
FROM BoroughBoundary;
Go
--- =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = City Council Boundary =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = ----
DROP TABLE  IF EXISTS CityCouncilBoundary
GO
CREATE TABLE CityCouncilBoundary
(

    District            INT PRIMARY KEY,
    CouncilGovernerName VARCHAR(255),
    BoroughID           INT FOREIGN KEY (BoroughID) REFERENCES BoroughBoundary (BoroughValue),
);
GO

CREATE OR ALTER PROCEDURE CityCouncilBoundary_Extraction AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;
    INSERT INTO CityCouncilBoundary (District, CouncilGovernerName, BoroughID)
    SELECT [DISTRICT], [NAME], bb.BoroughValue
    FROM [NYC_311_REQUESTS].[dbo].[Council_Members] nyc
    INNER JOIN BoroughBoundary bb ON [nyc].[BOROUGH] = bb.BoroughName
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC CityCouncilBoundary_Extraction;
GO

SELECT *
FROM CityCouncilBoundary;
GO
--- =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = Police Precint Boundary =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = ----
DROP TABLE IF EXISTS PolicePrecinct;
GO

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

	SET NOCOUNT ON;
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;
    INSERT INTO PolicePrecinct (PrecinctNumber, PrecinctName, PhoneNumber, PrecinctAddress, BoroughID)
    SELECT [Precinct_Number]
         , [Name]
         , [Phone_number]
         , [Address]
         , bb.BoroughValue
    FROM [NYC_311_REQUESTS].[dbo].[Police_Precint] nyc
             INNER JOIN BoroughBoundary bb ON [nyc].[Borough] = bb.BoroughName
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC PolicePrecinct_Extraction;
GO

SELECT * FROM PolicePrecinct;
GO
--- =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = Resolution TABLE =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = ----

DROP TABLE IF EXISTS Resolution;
GO

CREATE TABLE Resolution
(
    ID                          INT PRIMARY KEY IDENTITY,
    ServiceKeyID                INT UNIQUE,
    ResolutionDescription       VARCHAR(1000),
    ResolutionActionUpdatedDate DATETIME,
    ResolutionStatus            VARCHAR(255),
);
GO

CREATE OR ALTER PROCEDURE Resolution_Extract AS
BEGIN

	SET NOCOUNT ON;
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;
    INSERT INTO Resolution (ServiceKeyID, ResolutionDescription, ResolutionActionUpdatedDate, ResolutionStatus)
    SELECT Unique_Key
         , [Resolution_Description]
         , Resolution_Action_Updated_Date
         , [Status]
    FROM [NYC_311].[dbo].[311_JAN_2023_TO_MAR_2023] r
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC Resolution_Extract;
GO

SELECT *
FROM Resolution;
GO
--- =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = Service Request =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = ----

DROP TABLE IF EXISTS ServiceRequest;
GO

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
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;
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
    FROM [NYC_311].[dbo].[311_JAN_2023_TO_MAR_2023] r
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
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;
END
GO

EXEC ServiceRequests_Extract;
GO

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
	SET XACT_ABORT ON;
    DECLARE @RowCt INT = 0;

    BEGIN TRANSACTION;
    INSERT INTO ServiceRequest_Complaint (ServiceRequestID, ComplaintID)
    SELECT s.UniqueKey
         , c.ID
    FROM [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] r
             INNER JOIN ServiceRequest s
                        ON r.Unique_Key = s.ServiceKeyID
             INNER JOIN Complaint c
                        ON r.Complaint_Type = c.ComplaintType
	SET @RowCt += @@ROWCOUNT;

    IF @RowCt = 0
        BEGIN
            THROW 50001, 'No records found. Check with source system.', 1;
        END
    COMMIT TRANSACTION;	
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


------------------------------------------------------------------

--SELECT * FROM DBO.Agency				--15 
--SELECT * FROM DBO.COMPLAINT				--963 
--SELECT * FROM dbo.city					--100 
--SELECT * FROM dbo.Coordinates			--2,14,630 
--SELECT * FROM dbo.StreetAddress			--91,635 
--SELECT * FROM dbo.IncidentLocations		--7,18,793 
--SELECT * FROM dbo.BoroughBoundary		--5 
--SELECT * FROM dbo.CityCouncilBoundary	--49 
--SELECT * FROM DBO.PolicePrecinct		--77
--SELECT * FROM dbo.ServiceRequest
--SELECT distinCT ResolutionStatus FROM DBO.Resolution --7