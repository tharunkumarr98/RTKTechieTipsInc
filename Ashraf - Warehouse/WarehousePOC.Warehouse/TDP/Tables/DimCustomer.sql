CREATE TABLE [TDP].[DimCustomer] (

	[CustomerID] int NULL, 
	[CustomerName] varchar(100) NULL, 
	[Email] varchar(100) NULL, 
	[City] varchar(50) NULL, 
	[StartDate] date NULL, 
	[EndDate] date NULL, 
	[IsCurrent] bit NULL, 
	[ModifiedDate] datetime2(6) NULL, 
	[VersionNumber] int NULL
);