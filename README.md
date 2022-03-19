# HLTech

### Exercise One
![QuestionOneModel](https://user-images.githubusercontent.com/11217812/159111953-61e6dad7-25ec-4cdd-8649-fb353cf6735d.png)


### Exercise Two
```sql
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
```


### Exercise Three
```sql
SELECT  DISTINCT DimCustomer.CustomerKey
FROM    DimCustomer
LEFT OUTER JOIN    
        DimAccountCustomerBridge
ON      DimCustomer.CustomerKey = DimAccountCustomerBridge.CustomerKey
WHERE   DimAccountCustomerBridge.AccountKey IS NULL ;
```


### Exercise Four
![ExerciseFourModel](https://user-images.githubusercontent.com/11217812/159111955-e720c83f-1c8c-406d-90da-144927b7138b.png)


### Exercise Five
Where transformation is performed in flight (ETL). In order of investigation, eliminate the following causes:
- Observe the pipeline in a production-like environment to ascertain whether bottle necks are caused by badly performing automated tests etc rather than the data load itself.
- Ensure any indices on the target table are being dropped prior to loading as large indices can significantly slow down DML operations.
- Indices on source tables should be appropriately selected, and existing indices inspected for stale statistics.
- Check for any substantial increase in data volumes in the source system, particularly where a full extract is performed on each load. This could be determined by analysing the appropriate metadata columns. Altering the load to a delta and implementing a high water mark or Change Data Capture could yield a significant performance improvement.
- Pipelines should be analysed for long running transformations in a production-like environment. Overly complex transformations should be broken down into seperate set based operations where possible and performance monitored for improvement.
- Consider if there is any performance improvement 'pushing down' complex transformations to the target database (ELT).


Where staging or transformation is executed in the target system (ELT). In order of investigation, eliminate the following causes:
- Ensure any indices on landing entities are dropped prior to load.
- Ensure those indices are rebuilt post load and are tailored to downstream transformation logic.
- Run the underlying SQL from any transformation views or stored procedures, observing the execution plan for any obvious bottlenecks. Eliminate complex logic and data type conversions in join predicates where possible.
- Decompose the transformation logic into smaller set based operations if possible.
