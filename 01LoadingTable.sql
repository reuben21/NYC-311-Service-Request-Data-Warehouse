USE NYC_311_REQUESTS;
GO

--DROP TABLE ServiceRequests;
--GO

-- TO LOAD THE TABLE
CREATE TABLE DBO.ServiceRequests  (
    Unique_Key VARCHAR(50),
    Created_Date  VARCHAR(50),
    Closed_Date  VARCHAR(50),
    Agency VARCHAR(50),
    Agency_Name VARCHAR(100),
    Complaint_Type VARCHAR(100),
    Descriptor VARCHAR(100),
    Location_Type VARCHAR(100),
    Incident_Zip VARCHAR(10),
    Incident_Address VARCHAR(200),
    Street_Name VARCHAR(100),
    Cross_Street_1 VARCHAR(100),
    Cross_Street_2 VARCHAR(100),
    Intersection_Street_1 VARCHAR(100),
    Intersection_Street_2 VARCHAR(100),
    Address_Type VARCHAR(50),
    City VARCHAR(50),
    Landmark VARCHAR(100),
    Facility_Type VARCHAR(100),
    StatusName VARCHAR(50),
    Due_Date  VARCHAR(50),
    Resolution_Description VARCHAR(500),
    Resolution_Action_Updated_Date  VARCHAR(50),
    Community_Board VARCHAR(50),
    BBL VARCHAR(50),
    Borough VARCHAR(50),
    X_Coordinate_State_Plane VARCHAR(50),
    Y_Coordinate_State_Plane VARCHAR(50),
    Open_Data_Channel_Type VARCHAR(50),
    Park_Facility_Name VARCHAR(100),
    Park_Borough VARCHAR(50),
    Vehicle_Type VARCHAR(50),
    Taxi_Company_Borough VARCHAR(50),
    Taxi_Pick_Up_Location VARCHAR(100),
    Bridge_Highway_Name VARCHAR(100),
    Bridge_Highway_Direction VARCHAR(50),
    Road_Ramp VARCHAR(50),
    Bridge_Highway_Segment VARCHAR(100),
    Latitude VARCHAR(50),
    Longitude VARCHAR(50),
    LocationName VARCHAR(100),
    Zip_Codes VARCHAR(50),
    Community_Districts VARCHAR(50),
    Borough_Boundaries VARCHAR(50),
    City_Council_Districts VARCHAR(50),
    Police_Precincts VARCHAR(50)
);
GO

BULK INSERT ServiceRequest
FROM 'C:\SQL_Data\March2023.xlsx'
WITH (
   FORMAT = 'xlsx',
   FIRSTROW = 2,
   FIELDTERMINATOR = ',',
   ROWTERMINATOR = '\n',
   TABLOCK
);

sp_configure 'show advanced options', 1;
GO

sp_configure 'Ad Hoc Distributed Queries', 1;
GO
