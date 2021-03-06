/*
2. Using your model, write a SQL query to return the total account balance for each customer
where number of accounts is &gt; 1
*/
WITH MultipleAccounts AS 
(
    SELECT  AccountKey
            , CustomerKey
    FROM    DimAccountCustomerBridge
    WHERE   CustomerKey IN
            (
                -- Customers associated with multiple accounts at this time.
                SELECT  CustomerKey
                FROM    DimAccountCustomerBridge
                WHERE   EffectiveToDateTime > GETDATE() -- Active account associations only.
                GROUP BY
                        CustomerKey
                HAVING  COUNT(AccountKey) > 1
            )
    AND     EffectiveToDateTime > GETDATE()
)

SELECT  MultipleAccounts.CustomerKey
        ,SUM(FactAccountBalance.CurrentBalance)
FROM    MultipleAccounts
JOIN    FactAccountBalance
ON      FactAccountBalance.AccountKey = MultipleAccounts.AccountKey 
GROUP BY    
        MultipleAccounts.CustomerKey;