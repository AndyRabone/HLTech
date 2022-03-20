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

### Exercise 6
```python
# Author:   Andy Rabone
# Date:     20/03/2022
# Summary:  - Define archive and extracted files location after file download (default to current working directory)
#           - Housekeeping routine to remove any files from previous runs
#           - wget utility is used to grab the file from URL defined in name_link
#           - Check is made to check a file exists in target location (NB. HTTPError exception object should be 
#             caught and inspected in error handling around call of wget.Download() if raised. In practice, 
#             for transient server side errors exponential back-off would be implemented to facilitate retries.)
#           - Extraction of TSV file from Gzip archive on the filesystem, as per target_archive_location and 
#             target_file_location
#           - TSV file loaded to Pandas dataframe, and subset to living persons only.
#           - Subset filtered to primaryProfessions with 'producer' is found at index 0.
#           - Count taken of filtered subset, assigned to variable and returned in user friendly form to the terminal.

import wget
import pandas as pd
import gzip
import shutil
import os
import os.path

def file_cleanup(file_path):
    """
    Simple function to delete file where it already exists.
    """
    if os.path.exists(file_path):
        os.remove(file_path)


target_archive_location = f"{os.getcwd()}/name.basics.tsv.gz"
target_file_location = f"{os.getcwd()}/name.basics.tsv"
name_link = 'https://datasets.imdbws.com/name.basics.tsv.gz'

for file_path in (target_archive_location, target_file_location):
    file_cleanup(file_path)

wget.download(name_link, out=target_archive_location)

if not os.path.exists(target_archive_location):
    raise Exception(f"File '{name_link}' failed to download to the target location.")

with gzip.open(target_archive_location, 'rb') as f_in:
    with open(target_file_location, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

df = pd.read_csv(target_file_location, sep='\t')
df = df.query(r"deathYear == '\\N'", engine='python')

df = df[['primaryName','primaryProfession']]
producers = df[df['primaryProfession'].str.find('producer') == 0] 
producers_count = producers['primaryProfession'].count()

print(f"Living persons with 'Producer' as their first primary profession: {producers_count}")
```
