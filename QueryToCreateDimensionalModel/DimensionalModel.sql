CREATE DATABASE NYC_311_REQUESTS_DM;

USE NYC_311_REQUESTS_DM;
GO

-- Dimensional Modelling
CREATE TABLE DimDate (
   DateKey INT PRIMARY KEY,
   cdate DATETIME,
   cyear INT,
   cquarter INT,
   cmonth INT,
   day_of_week INT,
   chour INT,
   cminute INT,
   csecond INT
);

CREATE TABLE DimLocation (
	LocationKey INT PRIMARY KEY,
    LocationBorough VARCHAR(255),
    LocationIncidentZip VARCHAR(255),
    LocationLatitude FLOAT,
    LocationLongitude FLOAT
);
GO

CREATE TABLE DimAgency (
	AgencyKey INT PRIMARY KEY,
	AgencyName VARCHAR(255),
	AgencyDescription VARCHAR(255)
);
GO

CREATE TABLE DimComplaint (
	ComplaintKey INT PRIMARY KEY,
	ComplaintType VARCHAR(255),
	ComplaintDescription VARCHAR(MAX),
	ComplaintLocationType VARCHAR(255)
);
GO

CREATE TABLE DimStatus (
	StatusKey INT PRIMARY KEY,
	StatusName VARCHAR(255),
	StatusResolutionDescription VARCHAR(MAX),
	StatusResolutionActionUpdatedDate DATETIME,
	StatusStartDate DATE,
    StatusEndDate DATE,
);
GO

--Fact Table:

CREATE TABLE FactComplaint (
	DateKey INT,
	LocationKey INT,
	AgencyKey INT,
	ComplaintKey INT,
	StatusKey INT,
	VehicleCount INT,
	BridgeCount INT,
	IncidentCount INT,
	FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
	FOREIGN KEY (LocationKey) REFERENCES DimLocation(LocationKey),
	FOREIGN KEY (AgencyKey) REFERENCES DimAgency(AgencyKey),
	FOREIGN KEY (ComplaintKey) REFERENCES DimComplaint(ComplaintKey),
	FOREIGN KEY (StatusKey) REFERENCES DimStatus(StatusKey)
);
GO

