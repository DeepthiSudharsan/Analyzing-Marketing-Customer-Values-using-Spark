## Analyzing Marketing Customer Value Data using Apache Spark 
### ------------------- (Using only Scala map-reduce Spark API) -------------------

### DATA 

#### Data used for this project is from Kaggle. The data can be downloaded from here :

https://www.kaggle.com/pankajjsh06/ibm-watson-marketing-customer-value-data

##### First two rows of the dataset after dropping columns that aren't being used in this project

| State      | Customer Lifetime Value | Response | Coverage | Education | Effective To Date | Employment | Gender | Marital Status | Number of Policies | Policy       | Sales Channel | Total Claim | Vehicle Class | Vehicle Size |
|------------|-------------------------|----------|----------|-----------|-------------------|------------|--------|----------------|--------------------|--------------|---------------|-------------|---------------|--------------|
| Washington | 2763.519                | No       | Basic    | Bachelor  | 2/24/2011         | Employed   | F      | 56274          | 1                  | Corporate L3 | Agent         | 384.8111    | Two-Door Car  | Medsize      |
| Arizona    | 6979.536                | No       | Extended | Bachelor  | 1/31/2011         | Unemployed | F      | 0              | 8                  | Personal L3  | Agent         | 1131.465    | Four-Door Car | Medsize      |

***

### HOW TO RUN?

#### Once the data and the scala codes are all downloaded in the same place, run the loadproject.scala using the command 
```
:load loadproject.scala
```
#### This file will run the other 3 scala codes i.e.,
#### The reading data code (readdata.scala)
#### Analysis of Customer Information (analysiscustomer.scala)
#### Analysis of Company Information (analysiscompany.scala)

### The codes have been annotated and the outputs have also been commented out for reference


