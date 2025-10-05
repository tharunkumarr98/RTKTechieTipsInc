-- Create the stored procedure
CREATE PROCEDURE TDP.UpsertStagingCustomers_SCD2
AS
BEGIN
    --Improves performance and prevents server to send unnecessary messages like "X rows effected" should be used in stored procedures or batch statements
    SET NOCOUNT ON;

  
    -- Step 1: Insert new versions for changed records
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
    GETDATE(),           -- StartDate
    NULL,                -- EndDate
    1,                   -- IsCurrent
    GETDATE(),           -- ModifiedDate
    D.LatestVersion + 1  -- Proper next version
FROM StagingCustomer S
JOIN (
    SELECT CustomerID, MAX(VersionNumber) AS LatestVersion
    FROM DimCustomer
    GROUP BY CustomerID
) D ON S.CustomerID = D.CustomerID
JOIN DimCustomer DC ON DC.CustomerID = S.CustomerID AND DC.IsCurrent = 1
WHERE 
    S.CustomerName <> DC.CustomerName OR
    S.Email        <> DC.Email OR
    S.City         <> DC.City;
    
      -- Step 2: Expire old records in DimCustomer if changes are detected
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

    
    
    -- Step 3: Insert brand new customers (not in DimCustomer at all)
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
        GETDATE(),           -- StartDate
        NULL,                -- EndDate
        1,                   -- IsCurrent
        GETDATE(),           -- ModifiedDate
        1                    -- First version
    FROM StagingCustomer S
    LEFT JOIN DimCustomer D ON S.CustomerID = D.CustomerID
    WHERE D.CustomerID IS NULL;


   --Step 4 Update Deleted Records 

    Update D
    Set 
    D.EndDate = GETDATE(),
    D.IsCurrent = 0,
    D.ModifiedDate = GETDATE()
    from TDP.DimCustomer D LEFT join TDP.StagingCustomer S on D.CustomerID = S.CustomerID where S.CustomerID is null and D.IsCurrent = 1;
    
END;