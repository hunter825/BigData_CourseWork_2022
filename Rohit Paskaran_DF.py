# Databricks notebook source
# MAGIC %md
# MAGIC Making Reusable Code

# COMMAND ----------

#Reusable Code 

data = 2021

# COMMAND ----------

# MAGIC %md
# MAGIC Assigning_Values_toVariables

# COMMAND ----------

from pathlib import Path

clinicaltrial_2021  = spark.read.options(delimiter="|",header =True) \
  .csv("/FileStore/tables/clinicaltrial_"+str(data)+".csv")


mesh = spark.read.option("header",True) \
     .csv('/FileStore/tables/mesh.csv')

pharma = spark.read.option("header",True) \
     .csv('/FileStore/tables/pharma.csv')

# COMMAND ----------

#Checking the Created DataFrame
clinicaltrial_2021.display()

# COMMAND ----------

# MAGIC %md
# MAGIC QUESTION 1 

# COMMAND ----------

#Assigning the selected column from the dataframe to a variable.
Total_Studies = clinicaltrial_2021.select(clinicaltrial_2021.Id).count()

#Listing it 
Total_Studies

# COMMAND ----------

# MAGIC %md
# MAGIC QUESTION 2 

# COMMAND ----------

#Assigning the Selected columns "Types" grouped it and Count it.
Total_Types = clinicaltrial_2021.select(clinicaltrial_2021.Type).groupBy("Type").count()

#Ordering it in Descending Order
Splitting_Types = Total_Types.orderBy("Count",ascending=False)

#Displaying all the Types using Show Function.
Splitting_Types.show()

# COMMAND ----------

# MAGIC %md
# MAGIC QUESTION 3 

# COMMAND ----------

#Importing the Split and Explode from sql Function 
from pyspark.sql.functions import split, explode
#Splitting the Column Conditions with  "," using explode function
splitied_Conditions = clinicaltrial_2021.withColumn('Splitted_Conditions', explode(split(clinicaltrial_2021["Conditions"], ",")))
#Selecting  splitting conditions 
Splitting_Conditions  = splitied_Conditions.select(splitied_Conditions.Splitted_Conditions)
#Grouping it and ordering it 
Grouping = Splitting_Conditions.groupBy("Splitted_Conditions").count().orderBy("count", ascending = False)
#Showing the top 5 
Grouping.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC QUESTION 4 

# COMMAND ----------

# MAGIC %md
# MAGIC JOINNING MESH AND MAIN DF

# COMMAND ----------

#Splitting the values.
from pyspark.sql.functions import split, explode
splited_Value =clinicaltrial_2021.withColumn('Conditions',explode(split('Conditions',',')))

# COMMAND ----------

#Importing the sql Function col and substring
from pyspark.sql.functions import col, substring
#Selecting only the term and tree with  substring splitting only the first three as Root
Mesh_Splited  = mesh.select('term' , 'tree' , substring('tree', 1,3).alias('root'))
#Now using Inner Join merging the two tables ClinicalTrial and Mesh_Splited where conditions and terms are equal
new_joined_table = splited_Value.join(Mesh_Splited,splited_Value.Conditions ==  Mesh_Splited.term,"inner")
#Selecting the Root and grouping it with count
total_types = new_joined_table.select('root').groupBy("root").count()
#Ordering it in Descending Order
arranging_order = total_types.orderBy("Count",ascending=False)
#Displaying only top 5 
arranging_order.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC QUESTION 5 

# COMMAND ----------

# MAGIC %md
# MAGIC JOINNING PHARAMA AND MAIN DF

# COMMAND ----------

#Importing the sql Function col and substring
from pyspark.sql.functions import col, substring
#Now using Left Join merging the two tables ClinicalTrial and pharma where Sponsor and pharam are equal
new_pharma_table = clinicaltrial_2021.join(pharma,clinicaltrial_2021.Sponsor ==  pharma.Parent_Company,"left")
#Filtering out the Null Values
filtering = new_pharma_table.filter(pharma.Parent_Company.isNull())
#Selecting the Sponsor and Grouping it with a  count
total_types = filtering.select('Sponsor').groupBy("Sponsor").count()
#Ordering it by Descending Order using Count
total_types_spliting = total_types.orderBy("Count",ascending=False)
#Taking only the Top 10 with take Funtion
total_types_spliting.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC QUESTION 6 

# COMMAND ----------

#Importing the  dateformat Libraries from the SQL
import pyspark.sql.functions as F
from pyspark.sql.functions import year
from pyspark.sql.functions import substring
from pyspark.sql.functions  import date_format, to_date
from pyspark.sql.functions import month

#Filtering only the Completed  values from the Status Column
Completed_Month = clinicaltrial_2021.filter(clinicaltrial_2021.Status == "Completed") 
#Changing the datatype of the Completetion  column into TimeStamp 
Changing_dft= Completed_Month.select("Completion","Status",date_format(to_date("Completion", "MMM yyyy"),"MM-dd-yyyy").alias("ifrs_year_dt"))
#Selecting only the Completion , status and the TimeStamp Converted  Completetion Columns
total_types = Changing_dft.select('Completion','Status','ifrs_year_dt')
#Selecting only the particular year 2021 with the use of substring
selecting_year = total_types.where(substring("Completion",5,8) == "2021")
#Selecting only the month  value (frist three digit) from the Completion column  using the substring
selected_month_date = selecting_year.select(substring("Completion",1,3).alias("month"),"Status","ifrs_year_dt")
#Selecting month and the TimeStamp Converted  Completetion Column
Grouping = selected_month_date.select("month","ifrs_year_dt")
#Counting and order it by TimeStamp Converted  Completetion Column in ascending order with count
Grouping_1 = Grouping.groupBy("month","ifrs_year_dt").count().orderBy("ifrs_year_dt",ascending = True)
#Now selecting only the month and the newly created Count Column
Listing = Grouping_1.select("month","count")
#Finally displaying the values in month wise
Listing.show()

# COMMAND ----------

#Converting the df to pandas DF
Pandas_Conversion  = Listing.toPandas()
#Importing the matplotlib library
import matplotlib.pyplot as plt

#Assigning the x values and y values with figure size ,kind of chart and  color
Pandas_Conversion.plot(x="month", y=["count"], kind="bar", figsize=(9, 8),color='blue')
#Giving it a Tittle
plt.title("Number of completed studies each month")
#Giving y label
plt.ylabel("number of completed")


#print bar graph
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

explode = (0,0, 0.5, 0,0,0,0,0,0,0)
Pandas_Conversion.groupby(['month']).sum().plot(kind='pie', y='count' ,figsize=(9, 20),explode=explode)

# COMMAND ----------

# MAGIC %md
# MAGIC Further Analysis
# MAGIC 
# MAGIC Comapnies that made Highest number of Offences related to health. 

# COMMAND ----------

#Selecting only the company and Offense Group from the list 
Selecting_Companies = pharma.select("Company","Offense_Group")
#Filtering the Offence Group column which has only healthrelated offences 
offences_Company = Selecting_Companies.filter(Selecting_Companies.Offense_Group == "healthcare-related offenses") 
#Getting the count of the companies that makes the offence 
grouping = offences_Company.groupby(offences_Company.Offense_Group,"Company").count()
#Ordering it based on the  counts of the offences
Ordering = grouping.orderBy("count",ascending = False)
#Selecting only the Company and count columns
filter_further = Ordering.select("Company", "count")
#Displaying only Top 10 companies list.
filter_further.show(10)

# COMMAND ----------


