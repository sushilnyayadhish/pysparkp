from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import array_contains, explode, current_date, date_format, current_timestamp
from pyspark.sql.functions import sum,avg,max,min,mean,count, round, split
from pyspark.sql.functions import expr

import time
start_time = time.time()

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
sc = spark.sparkContext

data = [('James','','Smith','1991-04-01','M',2500),
  ('Michael','Rose','','2000-05-19','M',2000),
  ('Robert','','Williams','1978-09-05','M',3000),
  ('Maria','Anne','Jones','1967-12-01','F',4500),
  ('Jen','Mary','Brown','1980-02-17','F',5000)
]

data2 = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]

data3 = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
]

data4 = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]

data5 = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Raman","Finance","CA",99000,40,24000), \
    ("Scott","Finance","NY",83000,36,19000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]

data6 = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]


dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]

data7 = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

data8 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]

data11 = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]







columns = ["firstname","middlename","lastname","dob","gender","salary"]

columns2 =  StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('gender', IntegerType(), True)
         ])

columns3 = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])


columns4= ["employee_name","department","salary"]

columns5= ["employee_name","department","state","salary","age","bonus"]

columns6 = ["employee_name","department","state","salary","age","bonus"]

columns7 = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

columns8 = ["dept_name","dept_id"]


columns9= ["employee_name","department","state","salary","age","bonus"]

columns10= ["employee_name","department","state","salary","age","bonus"]

columns11 = StructType([
    StructField("name",StringType(),True),
    StructField("languagesAtSchool",ArrayType(StringType()),True),
    StructField("languagesAtWork",ArrayType(StringType()),True),
    StructField("currentState", StringType(), True),
    StructField("previousState", StringType(), True)
  ])

spark = SparkSession.builder.master("local[*]").appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data=data, schema = columns)

df2 = spark.createDataFrame(data=data2, schema = columns2)

df3 = spark.createDataFrame(data=data3, schema = columns3)

df4 = spark.createDataFrame(data=data4, schema = columns4)

df5 = spark.createDataFrame(data=data5, schema = columns5)

df6 = spark.createDataFrame(data=data6, schema = columns6)

empdf = spark.createDataFrame(data=emp, schema = columns7)

deptdf = spark.createDataFrame(data=dept, schema = columns8)

df7 = spark.createDataFrame(data = data7, schema = columns9)

df8 = spark.createDataFrame(data = data8, schema = columns10)

df11 = spark.createDataFrame(data=data11,schema=columns11)


# df11.show()
# df11.printSchema()

#1. Change Data Type of column need to use cast() function, new col added with name "sal" if we explicitly type new name
#Else if same col name then existing col data type will change
res1 = df.withColumn("sal",col("salary").cast("integer"))

#2. Change multiple col data type
res2 = df.withColumn("salary",col("salary").cast("integer"))\
         .withColumn("dob",col("dob").cast("string"))

#3.Update the value of an Existing Column
res3 = df.withColumn("salary",col("salary")*100)

#4.Create a column from existing column Bonus from Salary
res4 = df.withColumn("bonus",col("salary")*0.10)

#5.Add a column using withColumn()
res5 = df.withColumn("company_name",lit("TCS"))

#6.Rename of existing col
res6 = df.withColumnRenamed("firstname","first_name")

#7.Drop column from data frame
res7 = df.drop("gender")

#8.Update column by using key word "when"
res8 = df.withColumn("gender",when(df.gender == "M","male") \
         .when((col("gender") =="F"),col("female")) \
         .otherwise(df.gender))

#9.Add new column in DF with all Null values  [Remember None  will give Null]
res9 = df.withColumn("empty",lit(None))

#10.Add column by "Concentating" existing column
res10 = df.withColumn("fullname",concat_ws(" ","firstname","middlename","lastname"))

#11.Using "when" & lit() add new column with specifuc some condition
res11 = df.withColumn("grade",when(df.salary >= 5000, lit("A")) \
                               .when((df.salary >=  3000) & (df.salary >=2500 ),lit("B")) \
                                .otherwise(lit("C") ))

#12. Uisng "Select" add column to DF, Remember columns specified after select syntax. Only shows them in the result
#after select alias must be use
res12 = df.select("salary",lit(0.3).alias("bonus"))

#13.Using SQL add column to DF        ==> spark.sql("select query")
df.createOrReplaceTempView("tab")
res13 = spark.sql("select firstname, salary, salary * 0.3 as Bonus from tab")
res14 = spark.sql("select firstname, salary, current_date() as today_date from tab")
res15 = spark.sql("select firstname, salary, "+" case salary when salary =5000 then 'A' "+" else 'B' END as grade from tab" )

#16.Rename column of nested StructType
res16 = df2.withColumn("firstname",df2.name.firstname) \
           .withColumn("middlename", df2.name.middlename) \
           .withColumn("lastname", df2.name.lastname).drop("name")

#17.Not equal conbditions using "filter"
res17 = df3.filter(df3.state != "OH")
res18 = df3.filter(~(df3.state == "OH"))

#18.Filter using SQL functions
res19 = df3.filter("state=='NY'")
res20 = df3.filter((df3.state=="NY") & (df3.gender == "M"))
res21 = df3.filter("state == 'NY' and gender =='M'" )

#19.Using isin() or not(~) isin function to check data in given list avail. if any of avail then print
list = ["OH","DE"]
res22 = df3.filter(df3.state.isin(list))
res23 = df3.filter(~(df3.state.isin(list)))

#20.Using keyword serach filter;    startswith("N"), endswith("N"), contains("N")
res24 = df3.filter(df3.state.startswith("N"))
res25 = df3.filter(df3.state.endswith("Y"))
res26 = df3.filter(df3.name.firstname.contains("M"))

#21.Using like and rlike function
res27 = df3.withColumn("Fname",df3.name.firstname).withColumn("Mname",df3.name.middlename).withColumn("Lname",df3.name.lastname).drop("name")
res28 = res27.filter(res27.Fname.like("%James%"))
res31 = df3.filter(df3.name.lastname.like("W%"))

#22.Using filter on array :      array_contains
res29 = df3.filter(array_contains(df3.languages,"Scala"))



#23.Filtering struct columns :
res30 = df3.filter(df3.name.lastname == "Williams")
res31 = df3.filter(df3.name.lastname.like("W%"))

#24.Very IMP: Remove duplicates records from DF: (It will delete 1st duplicates records keep the 2nd one)
res32 = df4.distinct()

#25.Calculate no. of records using print("statement", + str(df.distinct().count())
#print("noduplicates: "+ str(res32.count()))

#26.Using DropDuplicates we can drop duplicates records
res33 = df4.dropDuplicates()

#27.Using DropDuplicates on columns we can remove them
res34 = df4.dropDuplicates(["department","salary"])

#28.Sort(*cols)   sort
res35 = df5.sort("department", "state")

#29.OrderBy(*cols)
res36 = df5.orderBy("employee_name","state")

#30.By explicitly defining sort by desc or asc
res37 = df5.sort(df5.department.asc(),df5.salary.desc())

#31.Using SQL order by
df5.createOrReplaceTempView("tab")
res38 = spark.sql("select employee_name, department, state, salary, age, bonus from tab ORDER BY department asc ")

#32.Using groupBy on single column
res39 = df6.groupBy("department").sum("salary")
res40 =df6.groupBy("department").count()
res41 = df6.groupBy("department").mean("salary")
res42 = df6.groupBy("department").avg("salary")
res43 = df6.groupBy("department").mean("salary")

#33.Using groupBy on multiple column   ((IIIMMMPPP))
res44 = df6.groupby("department","state").sum("salary","bonus")

#34.Using groupBy on single col to get agg output with round & 2 decimal digit
res45 = df6.groupBy("department").agg(sum("salary").alias("sum_salary"), \
                                     round(avg("salary"),2).alias("avg_salary"), \
                                     round(mean("salary"),2).alias("mean_salary"), \
                                     max("salary").alias("max_salary"))

#35.GroupBy-->agg-->mean,max,sum,avg-->where
res46 = df6.groupBy("department").agg(sum("salary").alias("sum_salary"), \
                                     round(avg("salary"),2).alias("avg_salary"), \
                                     round(mean("salary"),2).alias("mean_salary"), \
                                     max("salary").alias("max_salary")) \
                                  .where(F.col("sum_salary") >= 50000)


##JOIN
#36.Inner Join on employee table ==> df7 and department table ==> df8
res47 = empdf.join(deptdf,empdf.emp_dept_id == deptdf.dept_id)

#37.Full outer Join
res48 = empdf.join(deptdf,empdf.emp_dept_id == deptdf.dept_id,"outer")

#38.Left outer join
res49 = empdf.join(deptdf,empdf.emp_dept_id == deptdf.dept_id,"left")
res50 = empdf.join(deptdf,empdf.emp_dept_id == deptdf.dept_id,"leftouter")

#39.Right outer join
res51 = empdf.join(deptdf,empdf.emp_dept_id == deptdf.dept_id,"right")
res52 = empdf.join(deptdf,empdf.emp_dept_id == deptdf.dept_id,"rightouter")

#40.Left semi join (like inner join but diff it takes all col from left, matched col from rigt only, ignores other)
res53 = empdf.join(deptdf,empdf.emp_dept_id == deptdf.dept_id,"leftsemi")

#41.Left anti join (returns only non matched columns from left dataset)
res54 = empdf.join(deptdf,empdf.emp_dept_id == deptdf.dept_id,"leftanti")

#42.Self join  (not practiced..!)
res55 = empdf.alias("emp1").join(empdf.alias("emp2"), \
                                 F.col("emp1.superior_emp_id") == F.col("emp2.emp_id"),"inner") \
                                 .select(F.col("emp1.emp_id"),F.col("emp1.name"), \
                                         F.col("emp2.emp_id").alias("superior_emp_id"),
                                         F.col("emp2.name").alias("superior_emp_name"))

#43.Using SQL join.
empdf.createOrReplaceTempView("emp")
deptdf.createOrReplaceTempView("dept")

res_join = spark.sql("select * from emp e, dept d where e.emp_Dept_id == d.dept_id")
res_join1 = spark.sql("select * from emp")


##UNion/UnionAll and distinct()
#44.Using union and UnionAll is same
res56 = df7.union(df8)
#print("count: " + str(res56.count()))

res57 = df7.union(df8)
#print("countAll :" + str(res57.count()))

#45.Merge without duplicates
res58 = df7.union(df8).distinct()
#print("distinct_count: " + str(res58.count()))


##Handling null dataFrame
data77 = "C:\\Users\\Sushil\\OneDrive\\Desktop\\row\\Dataset\\Sushil\\small_zipcode.csv"
df9 = spark.read.format("csv").option("header","true").option("infeSchema","true").load(data77)
# df9.show()
# df9.printSchema()

#Replace all null with 0
res59 = df9.fillna("0")

#Replace particular column null with 0
res60 = df9.fillna("0",subset="population")

res61 = df9.withColumn("population",F.col("population").cast("integer"))

#AS whole table in string need to change with the help of cast.  ==> string to integer & then apply fillna()
res62 = res61.fillna(0,subset="population")

#Replace with blank or empty
res63 = res61.fillna("")

#Replace with any alpha or any word
res64 = df9.fillna("blank")

##pivot() used after group by. to trapose one column into many column. Note may change data type.
res65 = df6.groupBy("department").pivot("state").sum("salary")

##partitionBy ==> write or save df of one/more than one column into many column based on partionBy key()
res66 = df6.write.option("header",True) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("/tmp/zipcodes-state")


##Array.  to convert array to any columns (Note:it will add no of rows) use "explode" with column
#explode
res67 = df11.withColumn("lan_school",explode(col("languagesAtSchool"))) \
            .withColumn("lang_work",explode(col("languagesAtWork"))).drop("languagesAtSchool","languagesAtWork")

#using concat_ws join two array of same data type
res68 = df11.withColumn("state",concat_ws(",","languagesAtSchool","languagesAtWork")) \
            .drop("languagesAtSchool","languagesAtWork")


res69 = df11.filter(array_contains(df11.languagesAtSchool,"Scala"))

# res69.show()
# res69.printSchema()


data99 = "C:\\Users\\Sushil\\OneDrive\\Desktop\\row\Dataset\\world_bank.json"

df99 = spark.read.format("json").option("header","true").option("inferSchema","true").load(data99)

res100 = df99.withColumn("majorsectorpercent",explode(F.col("majorsector_percent"))) \
              .withColumn("mjsectornamecode",explode(F.col("mjsector_namecode"))) \
              .withColumn("mjtheme1",explode(F.col("mjtheme"))) \
              .withColumn("mjthemenamecode",explode(F.col("mjtheme_namecode"))) \
              .withColumn("projectdoc",explode(F.col("projectdocs"))) \
              .withColumn("sector", explode(F.col("sector"))) \
              .withColumn("sectornamecode", explode(F.col("sector_namecode"))) \
              .withColumn("themenamecode", explode(F.col("theme_namecode"))) \
              .withColumn("projectabstract_cdata",F.col("project_abstract.cdata")) \
              .withColumn("sector1_Name",F.col("sector1.Name")) \
              .withColumn("sector1_Percent",F.col("sector1.Percent")) \
              .withColumn("sector2_Name",F.col("sector2.Name")) \
              .withColumn("sector2_Percent",F.col("sector2.Percent")) \
              .withColumn("sector3_Name",F.col("sector3.Name")) \
              .withColumn("sector3_Percent",F.col("sector3.Percent")) \
              .withColumn("sector4_Name",F.col("sector4.Name")) \
              .withColumn("sector4_Percent",F.col("sector4.Percent")) \
              .withColumn("theme1_Name",F.col("theme1.Name")) \
              .withColumn("theme1_Percent",F.col("theme1.Percent")) \
              .withColumn("majorsectorpercent_Name",F.col("majorsectorpercent.Name")) \
              .withColumn("majorsectorpercent_Percent",F.col("majorsectorpercent.Percent")) \
              .withColumn("mjsectornamecode_Code",F.col("mjsectornamecode.code")) \
              .withColumn("mjsectornamecode_Name",F.col("mjsectornamecode.name")) \
              .withColumn("mjthemenamecode_code",F.col("mjthemenamecode.code")) \
              .withColumn("mjthemenamecode_name",F.col("mjthemenamecode.name")) \
              .withColumn("projectdoc_DocDate",F.col("projectdoc.DocDate")) \
              .withColumn("projectdoc_DocType",F.col("projectdoc.DocType")) \
              .withColumn("projectdoc_DocTypeDesc",F.col("projectdoc.DocTypeDesc")) \
              .withColumn("projectdoc_DocURL",F.col("projectdoc.DocURL")) \
              .withColumn("projectdoc_EntityID",F.col("projectdoc.EntityID")) \
              .withColumn("sectornamecode_code",F.col("sectornamecode.code")) \
              .withColumn("sectornamecode_name", F.col("sectornamecode.name")) \
              .withColumn("themenamecode_code", F.col("themenamecode.code")) \
              .withColumn("themenamecode_name", F.col("themenamecode.name")) \
              .drop("sector2","theme1","sector3","themenamecode","sectornamecode","projectdoc","mjthemenamecode","mjsectornamecode","majorsectorpercent","sector4","project_abstract","sector1","majorsector_percent","mjsector_namecode","mjtheme","mjtheme_namecode","projectdocs","sector","sector_namecode","theme_namecode")

#
# #  .withColumn("id",split($"_id.$$oid",",")(1)) \
# df99.withColumn("first", split($"PlanID.$$numberLong", ",")(1))
#    .withColumn("id", split($"_id.$$oid", ",")(1))

#Map partition in DF
#ByDefault DF wont support for map, hence first convert Df to RDD
data101 = [('James','Smith','M',30),
  ('Anna','Rose','F',41),
  ('Robert','Williams','M',62),]

columns101 = ["firstname","lastname","gender","salary"]
df101 = spark.createDataFrame(data=data101, schema= columns101)

rdd2 = df101.rdd.map(lambda x:(x[0]+","+x[1],x[2],x[3]*2))
                         #OR#
rdd2 = df101.rdd.map(lambda x:(x["firstname"]+","+x["lastname"]+x["gender"]+x["salary"]*2))
                         #OR#
rdd2 = df101.rdd.map(lambda x: (x.firstname+","+x.lastname,x.gender,x.salary*2))
                        #OR By Using custom Function UDF#
def funct(x):
    firstname = x.firstname
    lastname = x.lastname
    name = firstname + "," + lastname
    gender = x.gender.lower()
    salary = x.salary*2
    return (name, gender, salary)
rdd3 = df101.rdd.map(lambda x: funct(x))

df102=rdd3.toDF(["name","gender","new_salary"])


##working with Map
data103 = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]
schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)
])

df103 = spark.createDataFrame(data=data, schema = schema)

res103 = df103.withColumn("hair",F.col("properties").getItem("hair"))\
               .withColumn("eye", F.col("properties").getItem("eye"))\
              .drop("properties")

res104 = df103.withColumn("hair",df.properties.getItem("hair")) \
  .withColumn("eye",df.properties.getItem("eye")) \
  .drop("properties") \





# res110.printSchema()
# res110.show(truncate=False)
# res110.explain()









print ("time elapsed: {:.2f}s".format(time.time() - start_time))