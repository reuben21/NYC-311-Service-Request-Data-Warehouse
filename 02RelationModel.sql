USE MASTER;
GO

--DROP DATABASE NYC_311_REQUESTS;
GO

CREATE DATABASE NYC_311_REQUESTS;
GO


USE NYC_311_REQUESTS;
GO

-- RELATIONAL MODEL

CREATE TABLE Agency (
    ID INT PRIMARY KEY,
    AgencyName VARCHAR(255),
	AgencyDescription VARCHAR(255),
);
GO

CREATE TABLE Complaint (
    ID INT PRIMARY KEY,
    ComplaintType VARCHAR(255),
    ComplaintDescriptor VARCHAR(MAX)
);
GO

CREATE TABLE Facility (
    ID INT PRIMARY KEY,
    FacilityType VARCHAR(255),
    FacilityName VARCHAR(255),
    Borough VARCHAR(255)
);
GO

CREATE TABLE ServiceRequestLocation (
    ID INT PRIMARY KEY,
    IncidentZip VARCHAR(255),
    IncidentAddress VARCHAR(255),
    StreetName VARCHAR(255),
    CrossStreet1 VARCHAR(255),
    CrossStreet2 VARCHAR(255),
    IntersectionStreet1 VARCHAR(255),
    IntersectionStreet2 VARCHAR(255),
    AddressType VARCHAR(255),
    City VARCHAR(255),
    Landmark VARCHAR(255),
    Latitude FLOAT,
    Longitude FLOAT,
    LocationType VARCHAR(255),
    FacilityID INT,
    FOREIGN KEY (FacilityID) REFERENCES Facility(ID)
);
GO

CREATE TABLE ServiceRequest (
    UniqueKey INT PRIMARY KEY,
    CreatedDate DATETIME,
    ClosedDate DATETIME,
    DueDate DATETIME,
    ResolutionDescription VARCHAR(MAX),
    ResolutionActionUpdatedDate DATETIME,
    [Status] VARCHAR(255),
    AgencyID INT,
    LocationID INT,
    FOREIGN KEY (AgencyID) REFERENCES Agency(ID),
    FOREIGN KEY (LocationID) REFERENCES ServiceRequestLocation(ID)
);
GO

CREATE TABLE Vehicle (
    ID INT PRIMARY KEY,
    [Type] VARCHAR(255),
    CompanyBorough VARCHAR(255),
    PickUpLocation VARCHAR(255)
);
GO

CREATE TABLE Boundary (
    ID INT PRIMARY KEY,
    Borough VARCHAR(255),
    CommunityBoard VARCHAR(255),
    CouncilDistrict VARCHAR(255),
    Precinct VARCHAR(255)
);
GO

CREATE TABLE Bridge (
    ID INT PRIMARY KEY,
    [Name] VARCHAR(255),
    Direction VARCHAR(255),
    Ramp VARCHAR(255),
    Segment VARCHAR(255),
    BoundaryID INT,
    FOREIGN KEY (BoundaryID) REFERENCES Boundary(ID)
);
GO

CREATE TABLE ServiceRequest_Complaint (
    ServiceRequestID INT,
    ComplaintID INT,
    PRIMARY KEY (ServiceRequestID, ComplaintID),
    FOREIGN KEY (ServiceRequestID) REFERENCES ServiceRequest(UniqueKey),
    FOREIGN KEY (ComplaintID) REFERENCES Complaint(ID)
);
GO

CREATE TABLE ServiceRequest_Vehicle (
    ServiceRequestID INT,
    VehicleID INT,
    PRIMARY KEY (ServiceRequestID, VehicleID),
    FOREIGN KEY (ServiceRequestID) REFERENCES ServiceRequest(UniqueKey),
    FOREIGN KEY (VehicleID) REFERENCES Vehicle(ID)
);
GO

CREATE TABLE ServiceRequest_Bridge (
    ServiceRequestID INT,
    BridgeID INT,
    PRIMARY KEY (ServiceRequestID, BridgeID),
    FOREIGN KEY (ServiceRequestID) REFERENCES ServiceRequest(UniqueKey),
    FOREIGN KEY (BridgeID) REFERENCES Bridge(ID)
);
GO
