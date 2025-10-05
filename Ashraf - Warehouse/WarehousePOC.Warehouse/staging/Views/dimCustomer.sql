-- Auto Generated (Do not modify) A88DE6B9975AABE7806E65094163F204B23E77F94F3031F8CFF2A346FBEA7742
CREATE   VIEW [staging].[dimCustomer] AS (SELECT *, CONVERT(VARCHAR(32), HASHBYTES('MD5', ISNULL(CAST( [NameStyle] AS NVARCHAR(100)), '') +
ISNULL(CAST( [Title] AS NVARCHAR(100)), '') +
ISNULL(CAST( [TotalChildren] AS NVARCHAR(100)), '') +
ISNULL(CAST( [BirthDate] AS NVARCHAR(100)), '') +
ISNULL(CAST( [FrenchEducation] AS NVARCHAR(100)), '') +
ISNULL(CAST( [FrenchOccupation] AS NVARCHAR(100)), '') +
ISNULL(CAST( [MiddleName] AS NVARCHAR(100)), '') +
ISNULL(CAST( [Phone] AS NVARCHAR(100)), '') +
ISNULL(CAST( [NumberCarsOwned] AS NVARCHAR(100)), '') +
ISNULL(CAST( [FirstName] AS NVARCHAR(100)), '') +
ISNULL(CAST( [HouseOwnerFlag] AS NVARCHAR(100)), '') +
ISNULL(CAST( [SpanishEducation] AS NVARCHAR(100)), '') +
ISNULL(CAST( [CustomerKey] AS NVARCHAR(100)), '') +
ISNULL(CAST( [CustomerAlternateKey] AS NVARCHAR(100)), '') +
ISNULL(CAST( [GeographyKey] AS NVARCHAR(100)), '') +
ISNULL(CAST( [Gender] AS NVARCHAR(100)), '') +
ISNULL(CAST( [Suffix] AS NVARCHAR(100)), '') +
ISNULL(CAST( [DateFirstPurchase] AS NVARCHAR(100)), '') +
ISNULL(CAST( [MaritalStatus] AS NVARCHAR(100)), '') +
ISNULL(CAST( [EnglishEducation] AS NVARCHAR(100)), '') +
ISNULL(CAST( [LastName] AS NVARCHAR(100)), '') +
ISNULL(CAST( [YearlyIncome] AS NVARCHAR(100)), '') +
ISNULL(CAST( [AddressLine1] AS NVARCHAR(100)), '') +
ISNULL(CAST( [CommuteDistance] AS NVARCHAR(100)), '') +
ISNULL(CAST( [EmailAddress] AS NVARCHAR(100)), '') +
ISNULL(CAST( [NumberChildrenAtHome] AS NVARCHAR(100)), '') +
ISNULL(CAST( [SpanishOccupation] AS NVARCHAR(100)), '') +
ISNULL(CAST( [AddressLine2] AS NVARCHAR(100)), '') +
ISNULL(CAST( [EnglishOccupation] AS NVARCHAR(100)), '')),2) AS [HASH KEY] FROM staging.dimcustomer)