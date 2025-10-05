CREATE   PROCEDURE UpsertStagingCustomers_SCD2
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Expire old rows in DimCustomer where changes are detected
    UPDATE D
    SET 
        EndDate = GETDATE(),
        IsCurrent = 0,
        ModifiedDate = GETDATE()
    FROM DimCustomer D
    INNER JOIN StagingCustomer S ON S.CustomerID = D.CustomerID
    WHERE D.IsCurrent = 1
      AND (
           S.CustomerName <> D.CustomerName OR
           S.Email        <> D.Email OR
           S.City         <> D.City
      );

    -- Step 2: Insert new or changed records with correct version number
    INSERT INTO DimCustomer (
        CustomerID,
        CustomerName,
        Email,
        City,
        StartDate,
        EndDate,
        IsCurrent,
        ModifiedDate,
        VersionNumber
    )
    SELECT 
        S.CustomerID,
        S.CustomerName,
        S.Email,
        S.City,
        GETDATE(),         -- StartDate
        NULL,              -- EndDate
        1,                 -- IsCurrent
        GETDATE(),         -- ModifiedDate
        ISNULL(V.LatestVersion, 0) + 1  -- VersionNumber
    FROM StagingCustomer S
    LEFT JOIN (
        SELECT CustomerID, MAX(VersionNumber) AS LatestVersion
        FROM DimCustomer
        GROUP BY CustomerID
    ) V ON S.CustomerID = V.CustomerID
    LEFT JOIN DimCustomer C
        ON S.CustomerID = C.CustomerID AND C.IsCurrent = 1
    WHERE C.CustomerID IS NULL
       OR S.CustomerName <> C.CustomerName
       OR S.Email <> C.Email
       OR S.City <> C.City;

    -- Step 3: Clear the staging table
    TRUNCATE TABLE StagingCustomer;
END;