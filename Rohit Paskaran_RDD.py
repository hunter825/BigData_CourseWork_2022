# Databricks notebook source
# MAGIC %pip install bokeh

# COMMAND ----------

# MAGIC %md
# MAGIC Reusable Code

# COMMAND ----------

# AWS STORAGE
data = 2021

# COMMAND ----------

# Creating a RDD with the CSV and Assigning to a Empty Variable
clinicaltrial_2021 = sc.textFile("/FileStore/tables/clinicaltrial_"+str(data)+".csv")
mesh = sc.textFile("/FileStore/tables/mesh.csv")
pharma = sc.textFile("/FileStore/tables/pharma.csv")

# COMMAND ----------

#Checking the Created RDD
clinicaltrial_2021.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC QUESTION 1

# COMMAND ----------

#Droping the First column using the Iteration  and negelecting the null values using if statement
Dropping_First_Column = clinicaltrial_2021.mapPartitionsWithIndex(lambda id_x, iter: list(iter)[1:] if(id_x == 0) else iter)
#Getting the Count of the Distinct 
Dropping_First_Column.distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC QUESTION 2 

# COMMAND ----------

#Getting the previous Dropped column and splitting it using lambda function
Looping_delimter = Dropping_First_Column.map(lambda line: line.split("|"))
#Getting only the types column
Selecting_types = Looping_delimter.map(lambda line: line[5])
#Adding a value 1 to all the values and using reduce by key function getting the count finally sorting it by descending order 
Highest_Value = Selecting_types.map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: -x[1])
#Collecting the values
Highest_Value.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC QUESTION 3 

# COMMAND ----------

#Getting the previous Dropped column and splitting it using lambda function
Splitting = Dropping_First_Column.map(lambda line: line.split("|"))
#Getting only the Condition columns 
Selecting_Conditions = Splitting.map(lambda line: line[7])
#Splitting it with ,
Splitting_Conditions = Selecting_Conditions.flatMap(lambda line: line.split(","))
#Adding a value 1 to all the values and using reduce by key function getting the count finally sorting it by descending order 
Counting_Top_Values = Splitting_Conditions.map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1],False)
#Getting the First Row
First_Element = Counting_Top_Values.first()
#Ignoring the First Row 
Removing_First_element = Counting_Top_Values.filter(lambda x: x != First_Element)
#Taking only 5 elements
Removing_First_element.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC QUESTION 4 

# COMMAND ----------

# MAGIC %md
# MAGIC JOINING MESH  FILE  

# COMMAND ----------

Mesh_File  = mesh.mapPartitionsWithIndex(lambda id_x, iter: list(iter)[1:] if(id_x == 0) else iter)
#Splitting  by ,
Splitting1 = mesh.map(lambda x: x.split(","))
#Getting the first and second column and only the first three characters
Splitting2 = Splitting1.map(lambda op : (op[0],op[1][:3]))
#Adding one to each values of conditions column  earlier created for Question 3 
Counting_Highest  = Splitting_Conditions.map(lambda x: (x,1))
#Joining the two tables and reducing it by key and sorting it in descending order by the count 
joining  = Counting_Highest.join(Splitting2).map(lambda d: (d[1][1], 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1], False)
#Taking only the first 5 values 
joining.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC QUESTION 5 

# COMMAND ----------

#Splitting the pharma dataset with , of the second column and replacing the quotes to empty value
Pharma_Files = pharma.map(lambda y : y.split(',')[1]).map(lambda x: x.replace('"',''))
#Slitting the values of the ClinicalTable
Main_data =  clinicaltrial_2021.map(lambda s : s.split("|"))
#Getting only  the second column from the previous step
Getting_Sponsor = Main_data.map(lambda t : t[1])
#Subtracting  the pharma table from the ClinicalTable and reducing it by key and sorting it in descending order by the count 
subtracting =  Getting_Sponsor.subtract(Pharma_Files).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1], False)
#Getting only the first 10 values
subtracting.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC QUESTION 6 

# COMMAND ----------

#Getting only the Completeion and status column from the clinical table
Month_year = clinicaltrial_2021.map(lambda x: x.split("|")).map(lambda y : (y[2],y[4]))
#Getting only the completed value from the columns
completed_list = Month_year.filter(lambda s :  'Completed' in s)
#Filtering it by year and adding value 1 to  each row and reducing it by adding up
extracted  = completed_list.map(lambda op :op[1]).filter(lambda s :'2021' in s).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
#Splitting by  empty space of column 1 
obligating = extracted.map(lambda x: (x[0].split(' ')[0], x[1]))
#Collecting the result 
obligating.collect()

# COMMAND ----------

#Importing calender library
import calendar
#enumurating it by month
d = {i:e for e,i in enumerate(calendar.month_abbr[1:],1)}

#sorting it with the created RDD
formation = obligating.sortBy(keyfunc=lambda x: d.get(x[0])).collect()
formation

# COMMAND ----------

#Converting back into Rdd
rdds = sc.parallelize(formation)


#Taking the Month as x value for the plotting  and converting into a list back 
x = rdds.map(lambda line: line[0])
month = x.collect()

#Taking the count as y value for the plotting  and converting into a list back 
y = rdds.map(lambda line: line[1])
count = y.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Plotting Charts with Bokeh

# COMMAND ----------

#Importing the additional Libraries from the bokeh package
from bokeh.io import curdoc
from bokeh.embed import components,file_html
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.palettes import Blues5

#Maping the Graph by cvalue and giving a tittle 
p = figure(x_range= month, height=250, title="Studies Completed in 2021 MonthWise",toolbar_location=None, tools="")
#Representing it  has a Vbar with  top  value ,width and color 
p.vbar(x=month, top=count, width=0.5,fill_color = '#4169e1')
#Giving the Xaxis label
p.xaxis.axis_label = 'Month'
#Giving the yaxis label
p.yaxis.axis_label = 'Count of Studies'
#Getting the result as html with the CDN value 
html = file_html(p,CDN,"plot")
curdoc().theme = 'dark_minimal'

#Displaying the html value
displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC Further Analysis
# MAGIC 
# MAGIC Finding the top 5 Interventions.

# COMMAND ----------

#Getting the previous Dropped column and splitting it using lambda function
Splitting = Dropping_First_Column.map(lambda line: line.split("|"))
#Getting only the Interventions  columns 
Selecting_Interventions = Splitting.map(lambda line: line[8]).flatMap(lambda line: line.split(",")).map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1],False)
#Getting the First Row
First_Element = Selecting_Interventions.first()
#Ignoring the First Row 
Removing_First_element = Counting_Top_Values.filter(lambda x: x != First_Element)
#Taking only 5 elements
Removing_First_element.take(5)

# COMMAND ----------


