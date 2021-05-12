// ANALYSIS OF CUSTOMER INFORMATION
println("=========================ANALYSIS OF CUSTOMER INFORMATION =========================")
// =========================ANALYSIS OF CUSTOMER INFORMATION =========================
// No of Customers from each state
val state = mcva_data.map(r => (r._1,1)).reduceByKey(_+_).sortBy(_._2,false)
// state: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[289] at sortBy at analysiscustomer.scala:25
state.toDF("State","No of customers").show
/*
+----------+---------------+
|     State|No of customers|
+----------+---------------+
|California|           3150|
|    Oregon|           2601|
|   Arizona|           1703|
|    Nevada|            882|
|Washington|            798|
+----------+---------------+
*/
// Marital Status of customers
val maritalstatus = mcva_data.map(r => (r._9,1)).reduceByKey(_+_).sortBy(_._2,false)
// maritalstatus: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[299] at sortBy at analysiscustomer.scala:25
maritalstatus.toDF("Marital Status","No of customers").show
/*
+--------------+---------------+
|Marital Status|No of customers|
+--------------+---------------+
|       Married|           5298|
|        Single|           2467|
|      Divorced|           1369|
+--------------+---------------+
*/
// Employment status of customers
val work = mcva_data.map(r => (r._7,1)).reduceByKey(_+_).sortBy(_._2,false)
// work: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[309] at sortBy at analysiscustomer.scala:25
work.toDF("Employment","No of customers").show
/*
+-------------+---------------+
|   Employment|No of customers|
+-------------+---------------+
|     Employed|           5698|
|   Unemployed|           2317|
|Medical Leave|            432|
|     Disabled|            405|
|      Retired|            282|
+-------------+---------------+
*/
// Level of education of customers
val education = mcva_data.map(r => (r._5,1)).reduceByKey(_+_).sortBy(_._2,false)
// education: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[319] at sortBy at analysiscustomer.scala:25
education.toDF("Education","No of customers").show
/*
+--------------------+---------------+
|           Education|No of customers|
+--------------------+---------------+
|            Bachelor|           2748|
|             College|           2681|
|High School or Below|           2622|
|              Master|            741|
|              Doctor|            342|
+--------------------+---------------+
*/
// Gender distribution of customers
val gender = mcva_data.map(r => (r._8,1)).reduceByKey(_+_).sortBy(_._2,false)
// gender: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[329] at sortBy at analysiscustomer.scala:25
gender.toDF("Gender","No of customers").show
/*
+------+---------------+
|Gender|No of customers|
+------+---------------+
|     F|           4658|
|     M|           4476|
+------+---------------+
*/
// Vehicle Class owned by customers
val vehicle_class = mcva_data.map(r => (r._14,1)).reduceByKey(_+_).sortBy(_._2,false)
// vehicle_class: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[339] at sortBy at analysiscustomer.scala:25
vehicle_class.toDF("Vehicle Class","No of customers").show
/*
+-------------+---------------+
|Vehicle Class|No of customers|
+-------------+---------------+
|Four-Door Car|           4621|
| Two-Door Car|           1886|
|          SUV|           1796|
|   Sports Car|            484|
|   Luxury SUV|            184|
|   Luxury Car|            163|
+-------------+---------------+
*/
// Type of vehicles owned by customers
val vehicle_type = mcva_data.map(r => (r._15,1)).reduceByKey(_+_).sortBy(_._2,false)
// vehicle_type: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[349] at sortBy at analysiscustomer.scala:25
vehicle_type.toDF("Vehicle Type","No of customers").show
/*
+------------+---------------+
|Vehicle Type|No of customers|
+------------+---------------+
|     Medsize|           6424|
|       Small|           1764|
|       Large|            946|
+------------+---------------+
*/
/*
---------------------------- CONCLUSIONS ------------------------------
-> Most of the customers hail from the state of California
-> Washington is the state with the least customers
-> Most customers are employed
-> A large number of customers have finished college or got their bachelor degrees
-> The number of doctors are relatively less
-> Most customers own a four-door car and very few customers own Luxury SUVs or Luxury cars
-> Midsize vehicles are the most common choice among the customers
*/