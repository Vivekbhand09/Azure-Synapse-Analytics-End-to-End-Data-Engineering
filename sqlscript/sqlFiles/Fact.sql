IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
WHERE t.name='FactTable' AND s.name='gold')
    CREATE EXTERNAL TABLE gold.FactTable
    WITH
    (
        LOCATION= 'FactOrders',
        DATA_SOURCE= gold_source,
        FILE_FORMAT= parquet
    )
    AS    
    SELECT 
        O.DimOrdersKey,
        C.DimCustomerKey,
        P.DimProductKey,
        G.DimGeographyKey,
        F.Quantity,
        F.UnitPrice,
        F.TotalAmount
    FROM
        silver.silverTable F
    LEFT JOIN 
        gold.DimOrders O ON F.OrderID=O.OrderID
    LEFT JOIN
        gold.DimCustomer C ON F.CustomerID=C.CustomerID
    LEFT JOIN 
        gold.DimProduct P ON F.ProductID=P.ProductID
    LEFT JOIN 
        gold.DimGeography G ON F.RegionID=G.RegionID AND F.Country=G.Country
