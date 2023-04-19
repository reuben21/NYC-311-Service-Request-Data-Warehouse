USE [NYC_311_REQUESTS]
GO
/****** Object:  Table [dbo].[Council_Members]    Script Date: 2023-04-19 10:22:39 AM ******/
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
/****** Object:  Table [dbo].[Police_Precint]    Script Date: 2023-04-19 10:22:39 AM ******/
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
