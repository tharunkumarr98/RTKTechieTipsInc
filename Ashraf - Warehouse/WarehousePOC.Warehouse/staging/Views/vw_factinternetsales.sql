-- Auto Generated (Do not modify) 8E25A23E9B368B3E64475BB3651E2812230B4933242980958978AB2BCC026A6F
CREATE   VIEW [staging].[vw_factinternetsales] AS ( SELECT *, CONVERT(VARCHAR(32), HASHBYTES('MD5', ISNULL(CAST( [ExtendedAmount] AS NVARCHAR(100)), '') +
ISNULL(CAST( [SalesAmount] AS NVARCHAR(100)), '') +
ISNULL(CAST( [DiscountAmount] AS NVARCHAR(100)), '') +
ISNULL(CAST( [PromotionKey] AS NVARCHAR(100)), '') +
ISNULL(CAST( [ShipDate] AS NVARCHAR(100)), '') +
ISNULL(CAST( [CurrencyKey] AS NVARCHAR(100)), '') +
ISNULL(CAST( [OrderDate] AS NVARCHAR(100)), '') +
ISNULL(CAST( [ShipDateKey] AS NVARCHAR(100)), '') +
ISNULL(CAST( [CarrierTrackingNumber] AS NVARCHAR(100)), '') +
ISNULL(CAST( [TaxAmt] AS NVARCHAR(100)), '') +
ISNULL(CAST( [TotalProductCost] AS NVARCHAR(100)), '') +
ISNULL(CAST( [DueDate] AS NVARCHAR(100)), '') +
ISNULL(CAST( [ProductStandardCost] AS NVARCHAR(100)), '') +
ISNULL(CAST( [SalesOrderLineNumber] AS NVARCHAR(100)), '') +
ISNULL(CAST( [SalesOrderNumber] AS NVARCHAR(100)), '') +
ISNULL(CAST( [UnitPriceDiscountPct] AS NVARCHAR(100)), '') +
ISNULL(CAST( [ProductKey] AS NVARCHAR(100)), '') +
ISNULL(CAST( [SalesTerritoryKey] AS NVARCHAR(100)), '') +
ISNULL(CAST( [OrderDateKey] AS NVARCHAR(100)), '') +
ISNULL(CAST( [CustomerKey] AS NVARCHAR(100)), '') +
ISNULL(CAST( [DueDateKey] AS NVARCHAR(100)), '') +
ISNULL(CAST( [Freight] AS NVARCHAR(100)), '') +
ISNULL(CAST( [RevisionNumber] AS NVARCHAR(100)), '') +
ISNULL(CAST( [CustomerPONumber] AS NVARCHAR(100)), '') +
ISNULL(CAST( [OrderQuantity] AS NVARCHAR(100)), '') +
ISNULL(CAST( [UnitPrice] AS NVARCHAR(100)), '')),2) AS [HASH KEY] FROM staging.[factinternetsales])