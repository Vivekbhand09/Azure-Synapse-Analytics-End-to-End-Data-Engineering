--------------------
---- DimCustomer
--------------------

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
WHERE t.name='DimCustomer' AND s.name='gold')
    CREATE EXTERNAL TABLE gold.DimCustomer
    WITH
    (
        LOCATION= 'DimCustomer',
        DATA_SOURCE= gold_source,
        FILE_FORMAT= parquet
    )
    AS  
    SELECT * , ROW_NUMBER() OVER (ORDER BY T.CustomerID ) as DimCustomerKey
    FROM 
    (
        SELECT
            DISTINCT
            CustomerID,
            CustomerName,
            CustomerEmail,
            Domain
        FROM 
            silver.silverTable
    ) T


--------------------
---- DimProducts
--------------------

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
WHERE t.name='DimProduct' AND s.name='gold')
    CREATE EXTERNAL TABLE gold.DimProduct
    WITH
    (
        LOCATION= 'DimProduct',
        DATA_SOURCE= gold_source,
        FILE_FORMAT= parquet
    )
    AS
    SELECT *, ROW_NUMBER() OVER (ORDER BY T2.ProductID ) as DimProductKey
    FROM
    (
        SELECT
        DISTINCT
        ProductID,
        ProductName,
        ProductCategory
    FROM 
        silver.silverTable
    ) T2


--------------------
---- DimGeography
--------------------

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
WHERE t.name='DimGeography' AND s.name='gold')
    CREATE EXTERNAL TABLE gold.DimGeography
    WITH
    (
        LOCATION= 'DimGeography',
        DATA_SOURCE= gold_source,
        FILE_FORMAT= parquet
    )
    AS
    SELECT *,  ROW_NUMBER() OVER (ORDER BY T3.RegionID ) as DimGeographyKey
    FROM 
    (
        SELECT
        DISTINCT
        RegionID,
        Country
    FROM 
        silver.silverTable
    ) T3


--------------------
---- DimOrders
--------------------

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id
WHERE t.name='DimOrders' AND s.name='gold')
    CREATE EXTERNAL TABLE gold.DimOrders
    WITH
    (
        LOCATION= 'DimOrders',
        DATA_SOURCE= gold_source,
        FILE_FORMAT= parquet
    )
    AS
    SELECT *, ROW_NUMBER() OVER (ORDER BY T4.OrderID ) as DimOrdersKey
    FROM
    (
        SELECT
        DISTINCT
        OrderID,
        CustomerID,
        CustomerName,
        CustomerEmail,
        Domain,
        ProductID,
        ProductName,
        ProductCategory,
        RegionID,
        Country
    FROM 
        silver.silverTable
    ) T4


