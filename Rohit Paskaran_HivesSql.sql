-- Databricks notebook source
-- MAGIC %md
-- MAGIC Assigning a Variabe for reusability

-- COMMAND ----------

set data = 2021;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating the Master table for answering Questions.

-- COMMAND ----------

-- Creating a external table with the exxtracted data 
CREATE EXTERNAL TABLE if not exists clinicaltrial_2021(id STRING, 
Sponsor string,
Status string, 
Start_date string,
Completion string,
Type string,
Submission string,
Conditions string,
Interventions string)  USING CSV
LOCATION "dbfs:/FileStore/tables/clinicaltrial_${hiveconf:data}.csv"
OPTIONS(delimiter "|",header "true");

-- COMMAND ----------

-- Checking 
select * from clinicaltrial_2021;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 1 

-- COMMAND ----------

-- getting the count of distinict id 
Select COUNT(distinct id) AS Number_of_Studies from clinicaltrial_2021

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 2 

-- COMMAND ----------

-- Selecting the type and getting the count from the clinicalTrialTable and Grouping it by type and Order it in Descening order
SELECT Type, count(*)
FROM clinicaltrial_2021
GROUP BY Type
ORDER BY count(*) desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 3 

-- COMMAND ----------

-- Creating a view with  Splitting the Conditions by , using explode split 
CREATE  View if not exists condition_table AS
SELECT explode(split(Conditions, ",")) as conditions
FROM clinicaltrial_2021;


-- Selecting only the Conditions counting it and grouping it and ordering it in descending order
select conditions,count(*)
from condition_table
group by conditions
order by count(*) desc
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 4 

-- COMMAND ----------

-- Creating an External Table mesh  from the Data  with options header removed and delimter
CREATE EXTERNAL TABLE if not exists mesh(
term string,
tree string)  USING CSV
LOCATION "dbfs:/FileStore/tables/mesh.csv"
OPTIONS(delimiter ",",header "true");

-- Creating an view  for inner joining the conditions table with condition as term equals Conditions
CREATE view if not exists meshmerge AS
SELECT * 
FROM mesh 
INNER JOIN condition_table
ON mesh.term = condition_table.conditions;


-- Selecting only the  left three values from the tree column and count it.Further Groped it and order it in Descending Order and Selecting only top 5 
SELECT LEFT(tree,3) AS root, count(*) as count_of_root
FROM meshmerge
GROUP BY root
ORDER BY count(*) DESC
LIMIT 5;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 5 

-- COMMAND ----------

-- Creating an External Table pharma  from the Data  provided with options header removed and delimter
CREATE EXTERNAL TABLE IF NOT EXISTS pharma( 
Company string,
Parent_Company string,
Penalty_Amount string,
Subtraction_From_Penalty string,
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting string,
Penalty_Year string ,
Penalty_Date string,
Offense_Group string,
Primary_Offense string,
Secondary_Offense string,
Description string,
Level_of_Government string ,
Action_Type string ,
Agency string,
Civil_or_Criminal string ,
Prosecution_Agreement string,
Court string,
Case_ID string ,
Private_Litigation_Case_Title string,
Lawsuit_Resolution string,
Facility_State string ,
City string,
Address string,
Zip string,
NAICS_Code string ,
NAICS_Translation string ,
HQ_Country_of_Parent string ,
HQ_State_of_Parent string ,
Ownership_Structure string,
Parent_Company_Stock_Ticker string ,
Major_Industry_of_Parent string ,
Specific_Industry_of_Parent string ,
Info_Source string ,
Notes string)USING CSV
LOCATION "dbfs:/FileStore/tables/pharma.csv"
OPTIONS(delimiter ",",header "true");

-- Creating a View with left joing the pharma table with a condition. 
CREATE view if not exists LEFT_JOIN AS
select * 
FROM clinicaltrial_2021
LEFT JOIN pharma
ON clinicaltrial_2021.Sponsor = pharma.Parent_Company;

-- Selecting only the Sponsor from the previos view ignoring the null values finally grouping it and order it in Descending order 
SELECT Sponsor, count(Sponsor) AS sponsor_count
FROM LEFT_JOIN where Parent_Company is null
GROUP BY Sponsor 
ORDER BY count(*) DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION  6 

-- COMMAND ----------

-- Creating by selecting the count and completetion from the clinicaltrial table and setting the condition where the year is 2021 and status is completed and grouping it. 
CREATE VIEW IF NOT EXISTS Completed_Month AS 
SELECT Completion,count(Id) as StudiesCount
From clinicaltrial_2021
where Completion like '%2021' and Status = 'Completed'
Group By Completion 


-- COMMAND ----------

-- Creating a View by taking the  first three values from the completion  column as month  and 5 - 8 value as year
Create view if not exists LEFT_JOIN_COMPLETION as 
Select LEFT(Completion,3) as month, substring(Completion,5,8) as year, Completion, StudiesCount From Completed_Month

-- COMMAND ----------

-- Creating a view to convert the previos view columns  datatype to timestamp and orderit.
Create View if not Exists  Time_Stamp AS 
select from_unixtime(unix_timestamp(concat('01-',month,'-',year),'dd-MMM-yyyy'),'dd-MM-yyyy') as Date_col, month,StudiesCount
from LEFT_JOIN_COMPLETION
order by substring(Date_col,4,5);

-- COMMAND ----------

-- Selecting only the month and the count from the previos view 
Select month, StudiesCount from Time_Stamp

-- COMMAND ----------

-- Plottoig a bar chart
Select month, StudiesCount from Time_Stamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Further Analysis 
-- MAGIC 
-- MAGIC Finding the  top 10 Sponsors count who failed in the Studies.

-- COMMAND ----------

SELECT Sponsor,count(Status) AS Status_Count
FROM clinicaltrial_2021 where Status  == "Withdrawn" or  Status  == "Terminated"
GROUP BY Sponsor 
ORDER BY Status_Count DESC LIMIT 10;

-- COMMAND ----------


