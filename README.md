# #Study-on-Involuntary-Denial-Boarding-and-Air-Traffic-Landing-in-the-United-States-
# #ETL(Extract Transform & Load) and Data Visualization
This study aims to understand the rise in denied boarding and difficulties with air traffic landings in relation to the growing air traffic and the airline challenges. Using ETL (Extract Transform & Load) and visualization techniques, the data pertaining to airlines, passengers, denied boarding, and aircraft landing information is examined to ascertain any relation between the attributes across the datasets. All the datasets are related and for the travelers from United States. Third quarter had the most passengers travelled. 
All the datasets considered for analysis in this study were obtained from the open-source US Data Gov website.
 Using an Application Program Interface (API), the data related to the Commercial Aviation - Involuntary Denied Boarding and Air Traffic Passenger Statistics are read into a Python data frames respectively. 
 # #Extract-
 After the data is read into Python, it is placed on the Mongo DB which is a noSQL database. MongoDB uses a flexible schema to store data. So, the user need not create a schema unlike traditional databases.  Each record in a MongoDB database is a document described in BSON, a binary representation of the data, as opposed to tables of rows and columns like SQL databases. The data stored on Mongo Db is then accessible to applications in JSON format. MongoDB's document database architecture makes it simple for programmers to store both structured and unstructured data. With the help of python libraries, the connection to Mongo DB is established using the local host and port 27017. For each dataset, a test database and associated collections are created. The documents from each dataset are then loaded into the corresponding collection. 
 # #Transform-
 The data in the documents is then acquired into a data frame in python to get it into a structured format. This structured data is then transformed and formatted for a better data analysis. Data pre-processing is a very important step for any analysis. Pre-processing data can increase the correctness and quality of a dataset, making it more reliable by removing missing or inconsistent data values brought on by human or computer mistake. It ensures consistency in data. 
• Null values indicate the missing values in data.  Null values in any field would never contribute any detail to the analysis. Additionally, they ass complexity and impact the performance of the analysis.  As a part of data-preprocessing, the fields that have NULL values are identified and replaced with NaN values for string fields and zero in case of a numeric field. Each field in each dataset is check if it has any duplicates and the true duplicates are removed.  
• As part of the pre-processing of the data, any unnecessary fields are also removed. Operating Airline IATA Code, Operating Airline IATA Code, Published Airline IATA Code, Boarding Area, and Published Airlines are dropped from the Air Traffic Passenger dataset. The Air Traffic landing dataset's Published Airline IATA Code, Operating Airline IATA Code, and Operating Airline IATA Code columns are removed since they include simply codes rather than the real names of the airlines, which are captured in specific fields. 
• Duplicates should be removed from the data because its presence can lead to inaccurate results. The duplicates from all datasets are removed. Fields containing NaN values are removed since they provide nothing useful to the study. 
• In order to effectively perform aggregations on the numeric fields, the data types are formatted based on the type of data stored in them. Year, month, day and quarter are extracted from the date fields and the period fields to visualize the data later depending on various intervals. In the dataset for Involuntary Denial Boarding, the amount fields were recorded in several fields. Aggregations were used to combine the data into a single field. 
# #Load-
The converted data is subsequently stored in Postgres, an ORDBMS (object-relational database management system). Both relational and object-oriented database functions are supported by this free and open-source database. One table is created per dataset based on the formatted data types to store the transformed data in the structured form. 
