-- Databricks notebook source
-- MAGIC %md
-- MAGIC SQL IMPLEMENTATION

-- COMMAND ----------

--Definition of variables for reusability

-- COMMAND ----------

SET hivevar:fileroot1 = '/FileStore/tables/clinicaltrial_2021.csv';

-- COMMAND ----------

SET hivevar:fileroot2 = '/FileStore/tables/pharma.csv';

-- COMMAND ----------

--Creating table for clinicaltrial

-- COMMAND ----------

DROP TABLE IF EXISTS fileroot1_table;

CREATE TABLE fileroot1_table
USING csv
OPTIONS (
  path=${hivevar:fileroot1},
  header='true',
  delimiter='|',
  mode='FAILFAST',
  inferSchema='true'
);

-- COMMAND ----------

--Creating table for pharma

-- COMMAND ----------

DROP TABLE IF EXISTS fileroot2_table;

CREATE TABLE fileroot2_table
USING csv
OPTIONS (
  path=${hivevar:fileroot2},
  header='true',
  delimiter=',',
  mode='FAILFAST',
  inferSchema='true'
);

-- COMMAND ----------

--Viewing the created tables

-- COMMAND ----------

SELECT * FROM fileroot1_table LIMIT 10

-- COMMAND ----------

SELECT * FROM fileroot2_table LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Question 1
-- Counting the number of distinct studies in the dataset

-- COMMAND ----------

SELECT DISTINCT COUNT(*) AS Studies
FROM fileroot1_table

-- COMMAND ----------

-- DBTITLE 1,Question 2
-- Listing all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type, ordered from most frequent to least frequent.

-- COMMAND ----------

SELECT Type, COUNT(Type) As Total
FROM fileroot1_table
GROUP BY Type
ORDER BY Total DESC

-- COMMAND ----------

-- DBTITLE 1,Question 3
-- The top 5 conditions (from Conditions) with their frequencies.

-- COMMAND ----------

SELECT Top_Five_Conditions, count(*) AS Total
FROM fileroot1_table
LATERAL VIEW explode(split(Conditions, ",")) AS Top_Five_Conditions
GROUP BY Top_Five_Conditions
ORDER BY Total DESC
LIMIT 5

-- COMMAND ----------

-- DBTITLE 1,Question 4
-- To find the 10 most common sponsors that are not pharmaceutical companies, along with the number of clinical trials they have sponsored. 

-- COMMAND ----------

SELECT fileroot1_table.sponsor, COUNT(*) AS frequency
FROM fileroot1_table
LEFT ANTI JOIN fileroot2_table
ON fileroot1_table.sponsor=fileroot2_table.Parent_company
GROUP BY fileroot1_table.sponsor
ORDER BY frequency DESC
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Question 5
-- Plot the number of completed studies each month in a given year

-- COMMAND ----------

--Creating a view for monthly view

CREATE OR REPLACE VIEW Completed_Studies_By_Month AS
SELECT SUBSTRING(Completion,1,3) AS Month, count(Completion) AS frequency
FROM fileroot1_table
WHERE status = 'Completed' 
AND Completion LIKE '%2021%'
GROUP BY Month
ORDER BY unix_timestamp(Month, 'MMM') 
LIMIT 12

-- COMMAND ----------

SELECT * FROM Completed_Studies_By_Month

-- COMMAND ----------

--Visualization of the result

-- COMMAND ----------

SELECT * FROM Completed_Studies_By_Month

-- COMMAND ----------

-- DBTITLE 1,Further Analysis
-- Let's do a review of the headquarter country of the parent company to confirm the top 10 companies sponsoring the projecst and their countries.

-- COMMAND ----------

--Combinning the sponsor column in the clinicaltrial table and the headquater country of the parent company based on their common variables, then group the resulting datasetbt the the sponsor and th headquarter country, counting the  frequency of each sponsor, and the result ordered by the frequency in desending order

-- COMMAND ----------

SELECT fileroot1_table.sponsor, fileroot2_table.HQ_Country_of_Parent, COUNT(fileroot1_table.sponsor) AS frequency
FROM fileroot1_table 
FULL JOIN fileroot2_table
ON fileroot1_table.sponsor=fileroot2_table.Parent_company
GROUP BY fileroot1_table.sponsor, fileroot2_table.HQ_Country_of_Parent
ORDER BY frequency DESC
LIMIT 10;

-- COMMAND ----------

--Visualiztaion of the result.
--Same approach as in the question five above is deployed.

-- COMMAND ----------

SELECT fileroot1_table.sponsor, fileroot2_table.HQ_Country_of_Parent, COUNT(fileroot1_table.sponsor) AS frequency
FROM fileroot1_table 
FULL JOIN fileroot2_table
ON fileroot1_table.sponsor=fileroot2_table.Parent_company
GROUP BY fileroot1_table.sponsor, fileroot2_table.HQ_Country_of_Parent
ORDER BY frequency DESC
LIMIT 10;

-- COMMAND ----------


