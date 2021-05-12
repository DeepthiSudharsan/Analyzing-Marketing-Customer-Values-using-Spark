// READING THE DATA 
// Parsing the csv file consisting of the Marketing Customer Values
// Dropping the row containing headers using mapPartitionsWithIndex
val mappartrdd = sc.textFile("MCVA.csv").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
// mappartrdd: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[228] at mapPartitionsWithIndex at readdata.scala:24
// Converting the data into appropriate types and representing it in a Dataframe
println("========================= MCVA DATA =========================")
val mcva_data = mappartrdd.map{l => 
	val tupd = l.split(',')
    val(st,clt,resp,cover,edu,etd) = (tupd(1),tupd(2).toFloat,tupd(3),tupd(4),tupd(5),tupd(6))
    val(emp,gen,ms,nop) =  (tupd(7),tupd(8),tupd(11),tupd(16).toInt)
    val(pol,sch,totalc,vc,vz) =  (tupd(18),tupd(20),tupd(21).toFloat,tupd(22),tupd(23))
    (st,clt,resp,cover,edu,etd,emp,gen,ms,nop,pol,sch,totalc,vc,vz)}
mcva_data.toDF("State","Customer Lifetime","Response","Coverage",
	"Education","Effective to date","Employment","Gender","Marital Status",
	"Number of policies","Policy",	"Sales Channel","Total claim","Vehicle class",
	"Vehicle size").show
/*
========================= MCVA DATA (After dropping) =========================
mcva_data: org.apache.spark.rdd.RDD[(String, Float, String, String, String, String, String, String, String, Int, String, String, Float, String, String)] = MapPartitionsRDD[229] at map at readdata.scala:25
+----------+-----------------+--------+--------+--------------------+-----------------+-------------+------+--------------+------------------+------------+-------------+-----------+-------------+------------+
|     State|Customer Lifetime|Response|Coverage|           Education|Effective to date|   Employment|Gender|Marital Status|Number of policies|      Policy|Sales Channel|Total claim|Vehicle class|Vehicle size|
+----------+-----------------+--------+--------+--------------------+-----------------+-------------+------+--------------+------------------+------------+-------------+-----------+-------------+------------+
|Washington|        2763.5193|      No|   Basic|            Bachelor|        2/24/2011|     Employed|     F|       Married|                 1|Corporate L3|        Agent|  384.81116| Two-Door Car|     Medsize|
|   Arizona|         6979.536|      No|Extended|            Bachelor|        1/31/2011|   Unemployed|     F|        Single|                 8| Personal L3|        Agent|   1131.465|Four-Door Car|     Medsize|
|    Nevada|        12887.432|      No| Premium|            Bachelor|        2/19/2011|     Employed|     F|       Married|                 2| Personal L3|        Agent|   566.4722| Two-Door Car|     Medsize|
|California|         7645.862|      No|   Basic|            Bachelor|        1/20/2011|   Unemployed|     M|       Married|                 7|Corporate L2|  Call Center|  529.88135|          SUV|     Medsize|
|Washington|        2813.6926|      No|   Basic|            Bachelor|         2/3/2011|     Employed|     M|        Single|                 1| Personal L1|        Agent|  138.13087|Four-Door Car|     Medsize|
|    Oregon|         8256.298|     Yes|   Basic|            Bachelor|        1/25/2011|     Employed|     F|       Married|                 2| Personal L3|          Web|  159.38304| Two-Door Car|     Medsize|
|    Oregon|        5380.8984|     Yes|   Basic|             College|        2/24/2011|     Employed|     F|       Married|                 9|Corporate L3|        Agent|      321.6|Four-Door Car|     Medsize|
|   Arizona|           7216.1|      No| Premium|              Master|        1/18/2011|   Unemployed|     M|        Single|                 4|Corporate L3|        Agent|   363.0297|Four-Door Car|     Medsize|
|    Oregon|        24127.504|     Yes|   Basic|            Bachelor|        1/26/2011|Medical Leave|     M|      Divorced|                 2|Corporate L3|        Agent|      511.2|Four-Door Car|     Medsize|
|    Oregon|         7388.178|      No|Extended|             College|        2/17/2011|     Employed|     F|       Married|                 8|  Special L2|       Branch|  425.52783|Four-Door Car|     Medsize|
|California|         4738.992|      No|   Basic|             College|        2/21/2011|   Unemployed|     M|        Single|                 3| Personal L3|        Agent|      482.4|Four-Door Car|       Small|
|California|         8197.197|      No|   Basic|             College|         1/6/2011|   Unemployed|     F|       Married|                 3| Personal L3|        Agent|      528.0|          SUV|     Medsize|
|California|         8798.797|      No| Premium|              Master|         2/6/2011|     Employed|     M|       Married|                 3|Corporate L1|        Agent|  472.02972|Four-Door Car|     Medsize|
|   Arizona|         8819.019|     Yes|   Basic|High School or Below|        1/10/2011|     Employed|     M|       Married|                 8|Corporate L3|       Branch|      528.0|          SUV|     Medsize|
|California|        5384.4316|      No|   Basic|             College|        1/18/2011|     Employed|     M|        Single|                 8|Corporate L3|  Call Center|  307.13913|Four-Door Car|     Medsize|
|    Oregon|         7463.139|      No|   Basic|            Bachelor|        1/17/2011|     Employed|     F|       Married|                 2|Corporate L2|       Branch|  42.920273|Four-Door Car|     Medsize|
|    Nevada|         2566.868|      No|   Basic|High School or Below|         2/6/2011|Medical Leave|     M|       Married|                 1| Personal L3|  Call Center|   454.2451| Two-Door Car|     Medsize|
|California|        3945.2417|      No|   Basic|             College|         1/5/2011|Medical Leave|     M|       Married|                 1| Personal L2|  Call Center|    647.442|          SUV|     Medsize|
|    Oregon|         5710.333|      No|   Basic|             College|        2/27/2011|     Employed|     M|       Married|                 7| Personal L2|       Branch|  308.98166|Four-Door Car|     Medsize|
|California|         8162.617|      No| Premium|High School or Below|        1/14/2011|     Employed|     F|       Married|                 3|Corporate L2|  Call Center|      484.8|Four-Door Car|       Small|
+----------+-----------------+--------+--------+--------------------+-----------------+-------------+------+--------------+------------------+------------+-------------+-----------+-------------+------------+
only showing top 20 rows
*/