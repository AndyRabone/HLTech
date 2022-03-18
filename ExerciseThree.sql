/*
3. Using your model, write a SQL query to return all the customers with no accounts
*/
SELECT  DISTINCT DimCustomer.CustomerKey
FROM    DimCustomer
LEFT OUTER JOIN    
        DimAccountCustomerBridge
ON      DimCustomer.CustomerKey = DimAccountCustomerBridge.CustomerKey
WHERE   DimAccountCustomerBridge.AccountKey IS NULL ;