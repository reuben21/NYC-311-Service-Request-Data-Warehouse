USE NYC_311_REQUESTS;
GO

--DROP TABLE ServiceRequests;
--GO

-- TO LOAD THE TABLE
CREATE TABLE Service_Requests_Rawdata (
    Unique_Key						VARCHAR(50),
    Created_Date					DATETIME,
    Closed_Date						DATETIME,
    Agency						VARCHAR(50),
    Agency_Name						VARCHAR(100),
    Complaint_Type					VARCHAR(100),
    Descriptor						VARCHAR(100),
    Location_Type					VARCHAR(100),
    Incident_Zip					VARCHAR(10),
    Incident_Address				        VARCHAR(200),
    Street_Name						VARCHAR(100),
    Cross_Street_1					VARCHAR(100),
    Cross_Street_2					VARCHAR(100),
    Intersection_Street_1			        VARCHAR(100),
    Intersection_Street_2			        VARCHAR(100),
    Address_Type					VARCHAR(50),
    City						VARCHAR(50),
    Landmark						VARCHAR(100),
    Facility_Type					VARCHAR(100),
    Status						VARCHAR(50),
    Due_Date						DATETIME,
    Resolution_Description			        VARCHAR(500),
    Resolution_Action_Updated_Date	                DATETIME,
    Community_Board					VARCHAR(50),
    BBL							VARCHAR(50),
    Borough						VARCHAR(50),
    X_Coordinate_State_Plane		                VARCHAR(50),
    Y_Coordinate_State_Plane		                VARCHAR(50),
    Open_Data_Channel_Type			        VARCHAR(50),
    Park_Facility_Name				        VARCHAR(100),
    Park_Borough					VARCHAR(50),
    Vehicle_Type					VARCHAR(50),
    Taxi_Company_Borough			        VARCHAR(50),
    Taxi_Pick_Up_Location			        VARCHAR(100),
    Bridge_Highway_Name				        VARCHAR(100),
    Bridge_Highway_Direction		                VARCHAR(50),
    Road_Ramp						VARCHAR(50),
    Bridge_Highway_Segment			        VARCHAR(100),
    Latitude						VARCHAR(50),
    Longitude						VARCHAR(50),
    LocationName					VARCHAR(100),
    Zip_Codes						VARCHAR(50),
    Community_Districts				        VARCHAR(50),
    Borough_Boundaries				        VARCHAR(50),
    City_Council_Districts			        VARCHAR(50),
    Police_Precincts				        VARCHAR(50)
);
GO

-- Indexing & Modifying Columns Dataset Columns
--
ALTER TABLE [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
    ADD CONSTRAINT PK_Unique_Key PRIMARY KEY (Unique_Key);
GO

ALTER TABLE [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
    ALTER COLUMN [Agency] VARCHAR(10);
GO

ALTER TABLE [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
    ALTER COLUMN [Incident_Zip] VARCHAR(10);
GO

ALTER TABLE [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023]
    ALTER COLUMN [Incident_Address] VARCHAR(50);
GO

CREATE NONCLUSTERED INDEX IX_Agency
    ON [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] (Agency);
CREATE NONCLUSTERED INDEX IX_IncidentZip
    ON [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] (Incident_Zip)
CREATE NONCLUSTERED INDEX IX_IncidentAddress
    ON [NYC_311_REQUESTS].[dbo].[311_JAN_2023_TO_MAR_2023] (Incident_Address);
