SELECT 
    *,
    CONVERT(VARCHAR(32), 
        HASHBYTES('MD5',
              ISNULL(CAST(CustomerKey AS NVARCHAR(100)), '') +
              ISNULL(CAST(GeographyKey AS NVARCHAR(100)), '') +
              ISNULL(CAST(CustomerAlternateKey AS NVARCHAR(100)), '') +
              ISNULL(CAST(Title AS NVARCHAR(100)), '') +
              ISNULL(CAST(FirstName AS NVARCHAR(100)), '') +
              ISNULL(CAST(MiddleName AS NVARCHAR(100)), '') +
              ISNULL(CAST(LastName AS NVARCHAR(100)), '') +
              ISNULL(CAST(NameStyle AS NVARCHAR(100)), '') +
              ISNULL(CAST(BirthDate AS NVARCHAR(100)), '') +
              ISNULL(CAST(MaritalStatus AS NVARCHAR(100)), '') +
              ISNULL(CAST(Suffix AS NVARCHAR(100)), '') +
              ISNULL(CAST(Gender AS NVARCHAR(100)), '') +
              ISNULL(CAST(EmailAddress AS NVARCHAR(100)), '') +
              ISNULL(CAST(YearlyIncome AS NVARCHAR(100)), '') +
              ISNULL(CAST(TotalChildren AS NVARCHAR(100)), '') +
              ISNULL(CAST(NumberChildrenAtHome AS NVARCHAR(100)), '') +
              ISNULL(CAST(EnglishEducation AS NVARCHAR(100)), '') +
              ISNULL(CAST(SpanishEducation AS NVARCHAR(100)), '') +
              ISNULL(CAST(FrenchEducation AS NVARCHAR(100)), '') +
              ISNULL(CAST(EnglishOccupation AS NVARCHAR(100)), '') +
              ISNULL(CAST(SpanishOccupation AS NVARCHAR(100)), '') +
              ISNULL(CAST(FrenchOccupation AS NVARCHAR(100)), '') +
              ISNULL(CAST(HouseOwnerFlag AS NVARCHAR(100)), '') +
              ISNULL(CAST(NumberCarsOwned AS NVARCHAR(100)), '') +
              ISNULL(CAST(AddressLine1 AS NVARCHAR(100)), '') +
              ISNULL(CAST(AddressLine2 AS NVARCHAR(100)), '') +
              ISNULL(CAST(Phone AS NVARCHAR(100)), '') +
              ISNULL(CAST(DateFirstPurchase AS NVARCHAR(100)), '') +
              ISNULL(CAST(CommuteDistance AS NVARCHAR(100)), '') +
              ISNULL(CAST(Inserted_at AS NVARCHAR(100)), '') +
              ISNULL(CAST(Inserted_by AS NVARCHAR(100)), '')
        ), 2
    ) AS hashkey
FROM staging.dimcustomer;

---------------
SELECT 
    *,
    CONVERT(VARCHAR(32), 
        HASHBYTES('MD5',
              ISNULL(CAST(ProductKey AS NVARCHAR(100)), '') +
              ISNULL(CAST(OrderDateKey AS NVARCHAR(100)), '') +
              ISNULL(CAST(DueDateKey AS NVARCHAR(100)), '') +
              ISNULL(CAST(ShipDateKey AS NVARCHAR(100)), '') +
              ISNULL(CAST(CustomerKey AS NVARCHAR(100)), '') +
              ISNULL(CAST(PromotionKey AS NVARCHAR(100)), '') +
              ISNULL(CAST(CurrencyKey AS NVARCHAR(100)), '') +
              ISNULL(CAST(SalesTerritoryKey AS NVARCHAR(100)), '') +
              ISNULL(CAST(SalesOrderNumber AS NVARCHAR(100)), '') +
              ISNULL(CAST(SalesOrderLineNumber AS NVARCHAR(100)), '') +
              ISNULL(CAST(RevisionNumber AS NVARCHAR(100)), '') +
              ISNULL(CAST(OrderQuantity AS NVARCHAR(100)), '') +
              ISNULL(CAST(UnitPrice AS NVARCHAR(100)), '') +
              ISNULL(CAST(ExtendedAmount AS NVARCHAR(100)), '') +
              ISNULL(CAST(UnitPriceDiscountPct AS NVARCHAR(100)), '') +
              ISNULL(CAST(DiscountAmount AS NVARCHAR(100)), '') +
              ISNULL(CAST(ProductStandardCost AS NVARCHAR(100)), '') +
              ISNULL(CAST(TotalProductCost AS NVARCHAR(100)), '') +
              ISNULL(CAST(SalesAmount AS NVARCHAR(100)), '') +
              ISNULL(CAST(TaxAmt AS NVARCHAR(100)), '') +
              ISNULL(CAST(Freight AS NVARCHAR(100)), '') +
              ISNULL(CAST(CarrierTrackingNumber AS NVARCHAR(100)), '') +
              ISNULL(CAST(CustomerPONumber AS NVARCHAR(100)), '') +
              ISNULL(CAST(OrderDate AS NVARCHAR(100)), '') +
              ISNULL(CAST(DueDate AS NVARCHAR(100)), '') +
              ISNULL(CAST(ShipDate AS NVARCHAR(100)), '') +
              ISNULL(CAST(Inserted_at AS NVARCHAR(100)), '') +
              ISNULL(CAST(Inserted_by AS NVARCHAR(100)), '')
        ), 2
    ) AS hashkey
FROM staging.factinternetsales;


