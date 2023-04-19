USE [NYC_311_REQUESTS]
GO
/****** Object:  Table [dbo].[Council_Members]    Script Date: 2023-04-19 10:30:11 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Council_Members](
	[NAME] [nvarchar](50) NOT NULL,
	[DISTRICT] [tinyint] NOT NULL,
	[BOROUGH] [nvarchar](50) NOT NULL,
	[POLITICAL_PARTY] [nvarchar](50) NOT NULL,
 CONSTRAINT [PK_Council_Members] PRIMARY KEY CLUSTERED 
(
	[DISTRICT] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Police_Precint]    Script Date: 2023-04-19 10:30:11 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Police_Precint](
	[Precinct_Number] [tinyint] NOT NULL,
	[Name] [nvarchar](50) NOT NULL,
	[Phone_number] [nvarchar](50) NOT NULL,
	[Address] [nvarchar](50) NOT NULL,
	[Borough] [nvarchar](50) NOT NULL,
 CONSTRAINT [PK_Police_Precint] PRIMARY KEY CLUSTERED 
(
	[Precinct_Number] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Christopher Marte', 1, N'Manhattan', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Carlina Rivera', 2, N'Manhattan', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Erik Bottcher', 3, N'Manhattan', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Keith Powers', 4, N'Manhattan', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Julie Menin', 5, N'Manhattan', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Gale Brewer', 6, N'Manhattan', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Shaun Abreu', 7, N'Manhattan', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Diana Ayala', 8, N'Manhattan and Bronx', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Kristin Richardson Jordan', 9, N'Manhattan', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Carmen De La Rosa', 10, N'Manhattan', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Eric Dinowitz', 11, N'Bronx', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Kevin Riley', 12, N'Bronx', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Marjorie Velázquez', 13, N'Bronx', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Pierina Ana Sanchez', 14, N'Bronx', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Oswald Feliz', 15, N'Bronx', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Althea Stevens', 16, N'Bronx', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Rafael Salamanca', 17, N'Bronx', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Amanda Farías', 18, N'Bronx', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Vickie Paladino', 19, N'Queens', N'Republican')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Sandra Ung', 20, N'Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Francisco Moya', 21, N'Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Tiffany Cabán', 22, N'Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Linda Lee', 23, N'Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'James F. Gennaro', 24, N'Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Shekar Krishnan', 25, N'Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Julie Won', 26, N'Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Nantasha Williams', 27, N'Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Adrienne Adams', 28, N'Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Lynn Schulman', 29, N'Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Robert Holden', 30, N'Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Selvena N. Brooks-Powers', 31, N'Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Joann Ariola', 32, N'Queens', N'Republican')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Lincoln Restler', 33, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Jennifer Gutiérrez', 34, N'Brooklyn and Queens', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Crystal Hudson', 35, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Chi Ossé', 36, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Sandy Nurse', 37, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Alexa Avilés', 38, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Shahana Hanif', 39, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Rita Joseph', 40, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Darlene Mealy', 41, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Charles Barron', 42, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Justin Brannan', 43, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Kalman Yeger', 44, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Farah Louis', 45, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Mercedes Narcisse', 46, N'Brooklyn', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Ari Kagan', 47, N'Brooklyn', N'Republican')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Inna Vernikov', 48, N'Brooklyn', N'Republican')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Kamillah Hanks', 49, N'Staten Island', N'Democrat')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'David Carr', 50, N'Staten Island', N'Republican')
INSERT [dbo].[Council_Members] ([NAME], [DISTRICT], [BOROUGH], [POLITICAL_PARTY]) VALUES (N'Joseph Borelli', 51, N'Staten Island', N'Republican')
GO
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (1, N'1st Precinct', N'212-334-0611', N'16 Ericsson Place', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (2, N'5th Precinct', N'212-334-0711', N'19 Elizabeth Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (3, N'6th Precinct', N'212-741-4811', N'233 West 10 Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (4, N'7th Precinct', N'212-477-7311', N'19 1/2 Pitt Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (5, N'9th Precinct', N'212-477-7811', N'321 East 5 Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (6, N'10th Precinct', N'212-741-8211', N'230 West 20th Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (7, N'13th Precinct', N'212-477-7411', N'230 East 21st Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (8, N'Midtown South Precinct', N'212-239-9811', N'357 West 35th Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (9, N'17th Precinct', N'212-826-3211', N'167 East 51st Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (10, N'Midtown North Precinct', N'212-767-8400', N'306 West 54th Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (11, N'19th Precinct', N'212-452-0600', N'153 East 67th Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (12, N'20th Precinct', N'212-580-6411', N'120 West 82nd Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (13, N'Central Park Precinct', N'212-570-4820', N'86th St & Transverse Road', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (14, N'23rd Precinct', N'212-860-6411', N'164 East 102nd Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (15, N'24th Precinct', N'212-678-1811', N'151 West 100th Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (16, N'25th Precinct', N'212-860-6511', N'120 East 119th Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (17, N'26th Precinct', N'212-678-1311', N'520 West 126th Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (18, N'28th Precinct', N'212-678-1611', N'2271-89 8th Avenue', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (19, N'30th Precinct', N'212-690-8811', N'451 West 151st Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (20, N'32nd Precinct', N'212-690-6311', N'250 West 135th Street', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (21, N'33rd Precinct', N'212-927-3200', N'2207 Amsterdam Avenue', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (22, N'34th Precinct', N'212-927-9711', N'4295 Broadway', N'Manhattan')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (23, N'40th Precinct', N'718-402-2270', N'257 Alexander Avenue', N'Bronx')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (24, N'41st Precinct', N'718-542-4771', N'1035 Longwood Avenue', N'Bronx')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (25, N'42nd Precinct', N'718-402-3887', N'830 Washington Avenue', N'Bronx')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (26, N'43rd Precinct', N'718-542-0888', N'900 Fteley Avenue', N'Bronx')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (27, N'44th Precinct', N'718-590-5511', N'2 East 169th Street', N'Bronx')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (28, N'45th Precinct', N'718-822-5411', N'2877 Barkley Avenue', N'Bronx')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (29, N'46th Precinct', N'718-220-5211', N'2120 Ryer Avenue', N'Bronx')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (30, N'47th Precinct', N'718-920-1211', N'4111 Laconia Avenue', N'Bronx')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (31, N'48th Precinct', N'718-299-3900', N'450 Cross Bronx Expressway', N'Bronx')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (32, N'49th Precinct', N'718-918-2000', N'2121 Eastchester Road', N'Bronx')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (33, N'50th Precinct', N'718-543-5700', N'3450 Kingsbridge Avenue', N'Bronx')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (34, N'52nd Precinct', N'718-220-5811', N'3016 Webster Avenue', N'Bronx')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (35, N'60th Precinct', N'718-946-3311', N'2951 West 8th Street', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (36, N'61st Precinct', N'718-627-6611', N'2575 Coney Island Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (37, N'62nd Precinct', N'718-236-2611', N'1925 Bath Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (38, N'63rd Precinct', N'718-258-4411', N'1844 Brooklyn Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (39, N'66th Precinct', N'718-851-5611', N'5822 16th Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (40, N'67th Precinct', N'718-287-3211', N'2820 Snyder Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (41, N'68th Precinct', N'718-439-4211', N'333 65th Street', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (42, N'69th Precinct', N'718-257-6211', N'9720 Foster Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (43, N'70th Precinct', N'718-851-5511', N'154 Lawrence Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (44, N'71st Precinct', N'718-735-0511', N'421 Empire Boulevard', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (45, N'72nd Precinct', N'718-965-6311', N'830 4th Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (46, N'73rd Precinct', N'718-495-5411', N'1470 East New York Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (47, N'75th Precinct', N'718-827-3511', N'1000 Sutter Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (48, N'76th Precinct', N'718-834-3211', N'191 Union Street', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (49, N'77th Precinct', N'718-735-0611', N'127 Utica Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (50, N'78th Precinct', N'718-636-6411', N'65 6th Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (51, N'79th Precinct', N'718-636-6611', N'263 Tompkins Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (52, N'81st Precinct', N'718-574-0411', N'30 Ralph Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (53, N'83rd Precinct', N'718-574-1605', N'480 Knickerbocker Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (54, N'84th Precinct', N'718-875-6811', N'301 Gold Street', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (55, N'88th Precinct', N'718-636-6511', N'298 Classon Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (56, N'90th Precinct', N'718-963-5311', N'211 Union Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (57, N'94th Precinct', N'718-383-3879', N'100 Meserole Avenue', N'Brooklyn')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (58, N'100th Precinct', N'718-318-4200', N'92-24 Rockaway Beach Boulevard', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (59, N'101st Precinct', N'718-868-3400', N'16-12 Mott Avenue', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (60, N'102nd Precinct', N'718-805-3200', N'87-34 118th Street', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (61, N'103rd Precinct', N'718-657-8181', N'168-02 P.O Edward Byrne Ave.', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (62, N'104th Precinct', N'718-386-3004', N'64-2 Catalpa Avenue', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (63, N'105th Precinct', N'718-776-9090', N'92-08 222nd Street', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (64, N'106th Precinct', N'718-845-2211', N'103-53 101st Street', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (65, N'107th Precinct', N'718-969-5100', N'71-01 Parsons Boulevard', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (66, N'108th Precinct', N'718-784-5411', N'5-47 50th Avenue', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (67, N'109th Precinct', N'718-321-2250', N'37-05 Union Street', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (68, N'110th Precinct', N'718-476-9311', N'94-41 43rd Avenue', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (69, N'111th Precinct', N'718-279-5200', N'45-06 215th Street', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (70, N'112th Precinct', N'718-520-9311', N'68-40 Austin Street', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (71, N'113th Precinct', N'718-712-7733', N'167-02 Baisley Boulevard', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (72, N'114th Precinct', N'718-626-9311', N'34-16 Astoria Boulevard', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (73, N'115th Precinct', N'718-533-2002', N'92-15 Northern Boulevard', N'Queens')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (74, N'120th Precinct', N'718-876-8500', N'78 Richmond Terrace', N'Staten Island')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (75, N'121st Precinct', N'718-697-8700', N'970 Richmond Avenue', N'Staten Island')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (76, N'122nd Precinct', N'718-667-2211', N'2320 Hylan Boulevard', N'Staten Island')
INSERT [dbo].[Police_Precint] ([Precinct_Number], [Name], [Phone_number], [Address], [Borough]) VALUES (77, N'123rd Precinct', N'718-948-9311', N'116 Main Street', N'Staten Island')
GO
