# Hey! Good to see you here!

This project is about structuring an analytical solution from identifying the data source to delivering the data to the end user!

In the "lab" file you will find the code that runs the table creation scripts, etl processes and some analyzes that can help with business issues.

Below you will find more information about building the solution and the line of thought to arrive at the final result.

## Data modeling
The data model presented below refers to the data sources of logs and sounds. Staging modeling represents the intermediate layer of data, where it will be loaded raw into Redshift databases.

After processing the data, they will be loaded into the data warehouse, which has the structure and approach shown in the "Star Schema modeling" diagram.

![Data modeling diagram](/assets/sparkfy_project.jpg)

## Modeling approach
This chapter will explain a bit more about the approach to build the solution for Sparkfy analysis.

#### Data sources
The initial data sources consist of json type files.
1. Songs - This dataset contains data on songs and their respective artists.
2. Logs - This dataset was generated using an event generator to simulate a music streaming app.

#### Staging modeling
After understanding the structure of the data sources, the approach of loading them raw into the staging area was used so that they could be easily explored.

To load the data, the COPY function was used, which is optimized and allows working in parallel with the ingestion of data from multiple files within Redshift.

#### ETL Process
Once the raw data is properly stored in the staging area, the SQL language is used to extract the data from these tables, transform and standardize it and subsequently insert it into the structured tables.

#### Data warehouse modeling
For analytical modeling, the star schema structural approach was used, resulting in 5 final tables.

1. fact_songplay - Fact table that will store the simulated streaming events from the Sparkfy app.
2. dim_users - Dimension table that will store the application's unique user data.
3. dim_song - Dimension table that will store the unique song's metadata.
4. dim_artist - Dimension table that will store metadata related to unique artists.
5. dim_time - Dimension table responsible for storing event date and time variations such as day, hour, month and year.

The data warehouse allows for an easy understanding of data, in addition to optimizing queries and analyzes as it consists of a low volume of relationships between tables and does not use advanced normal forms.

##### Data types
The following data types were used in the modeling considering table storage space, raw data accuracy and completeness, standardization, and ease of handling during end-user analyses.

1. TEXT
2. BIGINT
3. FLOAT
4. SMALLINT
5. DATETIME

Some tables allow null data to be displayed for the business and technology area to identify possible gaps that have not been noticed, which can generate doubts and implement data quality with the aim of enriching the final insights.

#### Analysis
Below you will find some business questions that are answered in the notebook named "business_analysis".

1. What is the time period analysed?
2. Which locations have the most users using the app?
3. What are the most played artists?
4. What are the most played artists segmented by location?
5. What are the most played songs?
6. What are the most played songs segmented by location?
7. How many users do we have for the different existing levels?
8. What are the most popular user agents?
9. What are the most used user agents segmented by user level?
