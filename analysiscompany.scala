// ANALYSIS OF COMPANY INFORMATION
println("========================= ANALYSIS OF COMPANY INFORMATION =========================")
// Total Claim vs Sales Channel and No of customers per channel
val tot_rounded = mcva_data.map(l =>( (((l._13)/100).round)*100,List(l._12)))
// Rounding off the total claim to the nearest 100
val total_range = tot_rounded.reduceByKey(_++_).sortBy(_._1).map(l => (l._1,l._2.groupBy(identity).mapValues(_.size)))
// Mapping the total claim to the list of sales channel and no of customers
println("Total Claim, Sales Channel => Number of Customers")
total_range.toDF().take(10).foreach(println)
/*
Total Claim,Sales Channel => Number of Customers
[0,Map(Agent -> 163, Branch -> 117, Call Center -> 89, Web -> 67)]
[100,Map(Branch -> 277, Agent -> 307, Web -> 127, Call Center -> 168)]
[200,Map(Agent -> 312, Branch -> 206, Call Center -> 180, Web -> 95)]
[300,Map(Branch -> 515, Agent -> 752, Web -> 300, Call Center -> 338)]
[400,Map(Branch -> 405, Agent -> 524, Web -> 211, Call Center -> 267)]
[500,Map(Branch -> 414, Agent -> 537, Web -> 190, Call Center -> 298)]
[600,Map(Branch -> 218, Agent -> 318, Web -> 117, Call Center -> 165)]
[700,Map(Agent -> 179, Branch -> 123, Web -> 79, Call Center -> 76)]
[800,Map(Branch -> 111, Agent -> 107, Web -> 39, Call Center -> 61)]
[900,Map(Branch -> 52, Agent -> 78, Call Center -> 44, Web -> 25)]
*/

// Sales channel vs Response
val sc_vs_res = mcva_data.map(l =>( l._12,List(l._3))).reduceByKey(_++_).map(l => (l._1,l._2.groupBy(identity).mapValues(_.size)))
println("Sales Channel,Response => Number of Customers")
sc_vs_res.toDF().take(sc_vs_res.count.toInt).foreach(println)
/*
Sales Channel,Response  => Number of Customers
[Web,Map(No -> 1169, Yes -> 156)]
[Branch,Map(No -> 2273, Yes -> 294)]
[Call Center,Map(No -> 1573, Yes -> 192)]
[Agent,Map(No -> 2811, Yes -> 666)]
state_vs_res: org.apache.spark.rdd.RDD[(String, scala.collection.immutable.Map[String,Int])] = MapPartitionsRDD[1528] at map at analysiscompany.scala:36
State, Response
*/
// State vs Response
val state_vs_res = mcva_data.map(l =>( l._1,List(l._3))).reduceByKey(_++_).map(l => (l._1,l._2.groupBy(identity).mapValues(_.size)))
// state_vs_res: org.apache.spark.rdd.RDD[(String, scala.collection.immutable.Map[String,Int])] = MapPartitionsRDD[118] at map at <console>:25
println("State, Response => Number of Customers")
state_vs_res.toDF().take(state_vs_res.count.toInt).foreach(println)
/*
State, Response => Number of Customers
[California,Map(No -> 2694, Yes -> 456)]
[Washington,Map(No -> 689, Yes -> 109)]
[Oregon,Map(No -> 2225, Yes -> 376)]
[Arizona,Map(No -> 1460, Yes -> 243)]
[Nevada,Map(No -> 758, Yes -> 124)]
*/
val poltype = mcva_data.map(r => (r._11,1)).reduceByKey(_+_).sortBy(_._2,false)
println("Total no of policy types provided by the company")
poltype.count
poltype.toDF("Type of policies","No of customers").show
/*
Total no of policy types provided by the company
res257: Long = 9
+----------------+---------------+
|Type of policies|No of customers|
+----------------+---------------+
|     Personal L3|           3426|
|     Personal L2|           2122|
|     Personal L1|           1240|
|    Corporate L3|           1014|
|    Corporate L2|            595|
|    Corporate L1|            359|
|      Special L2|            164|
|      Special L3|            148|
|      Special L1|             66|
+----------------+---------------+
*/
// Coverage type vs Total no of customers
val covtype = mcva_data.map(r => (r._4,1)).reduceByKey(_+_).sortBy(_._2,false)
// covtype: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[39] at sortBy at <console>:25
covtype.toDF("Coverage","No of customers").show
/*
+--------+---------------+
|Coverage|No of customers|
+--------+---------------+
|   Basic|           5568|
|Extended|           2742|
| Premium|            824|
+--------+---------------+
*/
// Popular coverage type vs Popular policy type count
mcva_data.map(s => (s._4,s._11)).filter{case (k,v) => (v == "Personal L3")&&(k == "Basic")}.count
// res8: Long = 2081
// Popular coverage type vs least popular policy type count
mcva_data.map(s => (s._4,s._11)).filter{case (k,v) => (v == "Special L1")&&(k == "Basic")}.count
// res10: Long = 38
// Responses vs No of customers
val respcount = mcva_data.map(r => (r._3,1)).reduceByKey(_+_).sortBy(_._2,false)
respcount.toDF("Responses","No of customers").show
/*
+---------+---------------+
|Responses|No of customers|
+---------+---------------+
|       No|           7826|
|      Yes|           1308|
+---------+---------------+
*/
// Effective to date vs no of customers
val etdcount = mcva_data.map(r => (r._6,1)).reduceByKey(_+_).sortBy(_._2,false)
etdcount.toDF("Effective to Date","No of customers").show
/*
+-----------------+---------------+
|Effective to Date|No of customers|
+-----------------+---------------+
|          1/10/11|            195|
|          1/27/11|            194|
|          2/14/11|            186|
|          1/26/11|            181|
|          1/17/11|            180|
|          1/19/11|            179|
|           1/3/11|            178|
|          1/31/11|            178|
|          1/20/11|            173|
|          2/26/11|            169|
|          1/28/11|            169|
|          2/19/11|            168|
|           1/5/11|            167|
|          2/27/11|            167|
|          1/11/11|            166|
|           2/4/11|            164|
|          2/28/11|            161|
|          2/10/11|            161|
|          1/21/11|            160|
|          1/29/11|            160|
+-----------------+---------------+
only showing top 20 rows
*/
// Total no of policies per customer vs no of customers
val polcount = mcva_data.map(r => (r._10,1)).reduceByKey(_+_).sortBy(_._2,false)
// polcount: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[78] at sortBy at <console>:25
polcount.toDF("Policy count","No of customers").show
/*
+------------+---------------+
|Policy count|No of customers|
+------------+---------------+
|           1|           3251|
|           2|           2294|
|           3|           1168|
|           7|            433|
|           9|            416|
|           4|            409|
|           5|            407|
|           8|            384|
|           6|            372|
+------------+---------------+
*/
// Total Claim vs Coverage
// Rounding off the Total claim to the nearest 100 and mapping it to the coverage types
val claim_vs_cov = mcva_data.map(l =>( (((l._13)/100).round)*100,
	List(l._4))).reduceByKey(_++_).sortBy(_._1).map(
	l => (l._1,l._2.groupBy(identity).mapValues(_.size)))
  println("Total Claim , Coverage => Number of Customers")
claim_vs_cov.toDF().take(10).foreach(println)
/*
Total Claim , Coverage => Number of Customers
[0,Map(Premium -> 20, Basic -> 277, Extended -> 139)]
[100,Map(Premium -> 59, Basic -> 590, Extended -> 230)]
[200,Map(Premium -> 41, Basic -> 568, Extended -> 184)]
[300,Map(Premium -> 34, Basic -> 1664, Extended -> 207)]
[400,Map(Premium -> 40, Basic -> 727, Extended -> 640)]
[500,Map(Premium -> 195, Basic -> 878, Extended -> 366)]
[600,Map(Premium -> 105, Basic -> 279, Extended -> 434)]
[700,Map(Premium -> 58, Basic -> 190, Extended -> 209)]
[800,Map(Premium -> 97, Basic -> 172, Extended -> 49)]
[900,Map(Premium -> 29, Basic -> 81, Extended -> 89)]
*/
// Function to preprocess the vehicle class data
def split_cond(x:String):String = {
      if(x.contains('-')) return x.split('-')(0)
      else return x
      }
// Vehicle size vs Vehicle class
val size_vs_door = mcva_data.map(l =>( l._15,List(split_cond(l._14)))).reduceByKey(_++_).map(
	l => (l._1,l._2.groupBy(identity).mapValues(_.size)))
println("Vehicle size,Vehicle Class => Number of Customers")
size_vs_door.toDF().take(size_vs_door.count.toInt).foreach(println)
/*
Vehicle size,Vehicle Class => Number of Customers
[Large,Map(Luxury SUV -> 18, SUV -> 167, Two -> 221, Sports Car -> 49, Four -> 475, Luxury Car -> 16)]
[Medsize,Map(Luxury SUV -> 125, SUV -> 1308, Two -> 1282, Sports Car -> 366, Four -> 3237, Luxury Car -> 106)]
[Small,Map(Luxury SUV -> 41, SUV -> 321, Two -> 383, Sports Car -> 69, Four -> 909, Luxury Car -> 41)]
*/
// Policy type vs Vehicle Class
val pol_vs_veh = mcva_data.map(l =>( l._11,List(split_cond(l._14)))).reduceByKey(_++_).map(
      l => (l._1,l._2.groupBy(identity).mapValues(_.size)))
println(("Policy Type,Vehicle Class => Number of Customers"))
pol_vs_veh.toDF().take(pol_vs_veh.count.toInt).foreach(println)
/*
Policy Type,Vehicle Class => Number of Customers
[Corporate L3,Map(Luxury SUV -> 13, SUV -> 213, Two -> 215, Sports Car -> 59, Four -> 496, Luxury Car -> 18)]
[Corporate L1,Map(Luxury SUV -> 7, SUV -> 69, Two -> 65, Sports Car -> 18, Four -> 193, Luxury Car -> 7)]
[Special L1,Map(SUV -> 15, Two -> 11, Sports Car -> 3, Four -> 36, Luxury Car -> 1)]
[Special L3,Map(Luxury SUV -> 1, SUV -> 33, Two -> 31, Sports Car -> 7, Four -> 72, Luxury Car -> 4)]
[Personal L2,Map(Luxury SUV -> 40, SUV -> 439, Two -> 452, Sports Car -> 112, Four -> 1051, Luxury Car -> 28)]
[Special L2,Map(Luxury SUV -> 5, SUV -> 26, Two -> 27, Sports Car -> 15, Four -> 89, Luxury Car -> 2)]
[Personal L1,Map(Luxury SUV -> 33, SUV -> 218, Two -> 257, Sports Car -> 70, Four -> 640, Luxury Car -> 22)]
[Corporate L2,Map(Luxury SUV -> 12, SUV -> 111, Two -> 128, Sports Car -> 27, Four -> 302, Luxury Car -> 15)]
[Personal L3,Map(Luxury SUV -> 73, SUV -> 672, Two -> 700, Sports Car -> 173, Four -> 1742, Luxury Car -> 66)]
*/
// Policy vs Customer Lifetime Values(CLV)
val pol_vs_clv = mcva_data.map(l => (l._2,l._11)).sortBy(_._1)
pol_vs_clv.toDF("Customer Lifetime Value","Policy").show
/*
+-----------------------+------------+
|Customer Lifetime Value|      Policy|
+-----------------------+------------+
|              1898.0077| Personal L2|
|              1898.6837| Personal L3|
|              1904.0009| Personal L1|
|              1918.1198| Personal L1|
|              1940.9812| Personal L3|
|              1994.7749| Personal L1|
|              2004.3507| Personal L3|
|              2004.3507|  Special L2|
|              2004.3507| Personal L3|
|              2004.3507|Corporate L3|
|              2004.3507|Corporate L3|
|              2004.3507| Personal L3|
|               2009.773| Personal L3|
|              2030.7837| Personal L2|
|               2034.993| Personal L1|
|              2050.6235| Personal L3|
|               2052.949| Personal L3|
|               2063.388| Personal L3|
|              2064.4587| Personal L3|
|               2064.698| Personal L2|
+-----------------------+------------+
*/
val cov_list = mcva_data.map(l => l._4).distinct.collect().toList
val state_list = mcva_data.map(l => l._1).distinct.collect().toList
val pol_list = mcva_data.map(l => l._11).distinct.collect().toList
//CLV vs state
val clv_st = mcva_data.map(l =>( (((l._2)/100).round)*100,List(l._1)))
val state_vs_clv = clv_st.reduceByKey(_++_).sortBy(_._1).map(l => (l._1,l._2.groupBy(identity).mapValues(_.size)))
println("CLV rounded , State => Number of Customers")
state_vs_clv.toDF().take(15).foreach(println)
/*
CLV rounded , State => Number of Customers
[1900,Map(Oregon -> 1, California -> 3, Arizona -> 1)]
[2000,Map(Oregon -> 5, Washington -> 1, California -> 3, Arizona -> 1)]
[2100,Map(California -> 8, Nevada -> 3, Washington -> 5, Arizona -> 3, Oregon -> 7)]
[2200,Map(California -> 24, Nevada -> 11, Washington -> 6, Arizona -> 13, Oregon -> 17)]
[2300,Map(California -> 25, Nevada -> 2, Washington -> 12, Arizona -> 15, Oregon -> 29)]
[2400,Map(California -> 66, Nevada -> 13, Washington -> 14, Arizona -> 51, Oregon -> 46)]
[2500,Map(California -> 81, Nevada -> 19, Washington -> 30, Arizona -> 51, Oregon -> 80)]
[2600,Map(California -> 86, Nevada -> 20, Washington -> 23, Arizona -> 43, Oregon -> 68)]
[2700,Map(California -> 65, Nevada -> 29, Washington -> 20, Arizona -> 40, Oregon -> 60)]
[2800,Map(California -> 66, Nevada -> 20, Washington -> 18, Arizona -> 35, Oregon -> 47)]
[2900,Map(California -> 57, Nevada -> 18, Washington -> 21, Arizona -> 30, Oregon -> 49)]
[3000,Map(California -> 23, Nevada -> 6, Washington -> 12, Arizona -> 14, Oregon -> 20)]
[3100,Map(California -> 27, Nevada -> 7, Washington -> 6, Arizona -> 15, Oregon -> 25)]
[3200,Map(California -> 25, Nevada -> 6, Washington -> 6, Arizona -> 19, Oregon -> 19)]
[3300,Map(California -> 16, Nevada -> 8, Washington -> 9, Arizona -> 10, Oregon -> 24)]
*/
// Policy type vs Total Claim
val pol_claim = mcva_data.map(l =>( (((l._13)/100).round)*100,List(l._11)))
val pol_vs_claim = pol_claim.reduceByKey(_++_).sortBy(_._1).map(l => (l._1,l._2.groupBy(identity).mapValues(_.size)))
println("Claim , Policy type => Number of Customers")
pol_vs_claim.toDF().take(pol_vs_claim.count.toInt).foreach(println)
/*
Claim , Policy type => Number of Customers
[0,Map(Corporate L1 -> 13, Special L3 -> 3, Personal L1 -> 67, Personal L2 -> 89, Corporate L3 -> 64, Personal L3 -> 159, Corporate L2 -> 29, Special L1 -> 4, Special L2 -> 8)]
[100,Map(Corporate L1 -> 30, Special L3 -> 18, Personal L1 -> 95, Personal L2 -> 217, Corporate L3 -> 105, Personal L3 -> 333, Corporate L2 -> 63, Special L1 -> 4, Special L2 -> 14)]
[200,Map(Corporate L1 -> 41, Special L3 -> 13, Personal L1 -> 130, Personal L2 -> 174, Corporate L3 -> 86, Personal L3 -> 276, Corporate L2 -> 43, Special L1 -> 6, Special L2 -> 24)]
[300,Map(Corporate L1 -> 67, Special L3 -> 29, Personal L1 -> 273, Personal L2 -> 450, Corporate L3 -> 210, Personal L3 -> 730, Corporate L2 -> 105, Special L1 -> 16, Special L2 -> 25)]
[400,Map(Corporate L1 -> 56, Special L3 -> 22, Personal L1 -> 169, Personal L2 -> 313, Corporate L3 -> 157, Personal L3 -> 556, Corporate L2 -> 107, Special L1 -> 5, Special L2 -> 22)]
[500,Map(Corporate L1 -> 65, Special L3 -> 28, Personal L1 -> 171, Personal L2 -> 344, Corporate L3 -> 151, Personal L3 -> 543, Corporate L2 -> 100, Special L1 -> 13, Special L2 -> 24)]
[600,Map(Corporate L1 -> 35, Special L3 -> 12, Personal L1 -> 121, Personal L2 -> 199, Corporate L3 -> 92, Personal L3 -> 285, Corporate L2 -> 52, Special L1 -> 3, Special L2 -> 19)]
[700,Map(Corporate L1 -> 16, Special L3 -> 7, Personal L1 -> 63, Personal L2 -> 105, Corporate L3 -> 43, Personal L3 -> 174, Corporate L2 -> 35, Special L1 -> 4, Special L2 -> 10)]
[800,Map(Corporate L1 -> 12, Special L3 -> 7, Personal L1 -> 49, Personal L2 -> 72, Corporate L3 -> 32, Personal L3 -> 123, Corporate L2 -> 14, Special L1 -> 4, Special L2 -> 5)]
[900,Map(Corporate L1 -> 9, Special L3 -> 1, Personal L1 -> 21, Personal L2 -> 54, Corporate L3 -> 27, Personal L3 -> 63, Corporate L2 -> 17, Special L1 -> 1, Special L2 -> 6)]
[1000,Map(Corporate L1 -> 4, Special L3 -> 2, Personal L1 -> 20, Personal L2 -> 33, Corporate L3 -> 17, Personal L3 -> 58, Corporate L2 -> 5, Special L1 -> 2, Special L2 -> 2)]
[1100,Map(Corporate L1 -> 3, Special L3 -> 2, Personal L1 -> 14, Personal L2 -> 13, Corporate L3 -> 6, Personal L3 -> 30, Corporate L2 -> 5, Special L1 -> 1, Special L2 -> 1)]
[1200,Map(Corporate L1 -> 1, Special L3 -> 1, Personal L1 -> 14, Personal L2 -> 10, Corporate L3 -> 3, Personal L3 -> 23, Corporate L2 -> 6, Special L1 -> 2)]
[1300,Map(Corporate L1 -> 1, Personal L1 -> 14, Personal L2 -> 24, Corporate L3 -> 7, Personal L3 -> 21, Corporate L2 -> 3, Special L1 -> 1, Special L2 -> 2)]
[1400,Map(Special L3 -> 2, Personal L1 -> 9, Personal L2 -> 9, Corporate L3 -> 7, Personal L3 -> 22, Corporate L2 -> 4, Special L2 -> 2)]
[1500,Map(Corporate L1 -> 1, Personal L1 -> 5, Personal L2 -> 3, Personal L3 -> 6, Corporate L2 -> 2)]
[1600,Map(Personal L1 -> 1, Corporate L2 -> 2, Personal L3 -> 7, Personal L2 -> 4)]
[1700,Map(Personal L1 -> 2, Personal L3 -> 2, Personal L2 -> 2)]
[1800,Map(Corporate L1 -> 1, Personal L1 -> 1, Personal L2 -> 1, Corporate L3 -> 2, Personal L3 -> 3, Corporate L2 -> 1)]
[1900,Map(Personal L3 -> 3)]
[2000,Map(Corporate L1 -> 1, Personal L1 -> 1, Personal L2 -> 3, Corporate L3 -> 1, Personal L3 -> 5)]
[2100,Map(Corporate L2 -> 1, Corporate L1 -> 1, Personal L3 -> 2, Personal L2 -> 1)]
[2200,Map(Corporate L2 -> 1, Personal L2 -> 1)]
[2300,Map(Special L3 -> 1, Corporate L1 -> 2, Personal L3 -> 1, Corporate L3 -> 2)]
[2500,Map(Corporate L3 -> 1)]
[2600,Map(Personal L3 -> 1)]
[2800,Map(Corporate L3 -> 1)]
[2900,Map(Personal L2 -> 1)]
*/
val clv_vs_all = mcva_data.map(l => (l._2,pol_list.indexOf(l._11),state_list.indexOf(l._1),
  cov_list.indexOf(l._4),l._10)).sortBy(_._1).toDF("CLV","Policy Type","State",
  "Coverage","Number of Policies")
  clv_vs_all.coalesce(1).write.option("header","true").csv("CLV_vs_all")
  clv_vs_all.show
/*
+---------+-----------+-----+--------+------------------+
|      CLV|Policy Type|State|Coverage|Number of Policies|
+---------+-----------+-----+--------+------------------+
|1898.0077|          4|    0|       0|                 1|
|1898.6837|          8|    3|       0|                 1|
|1904.0009|          6|    0|       0|                 1|
|1918.1198|          6|    0|       0|                 1|
|1940.9812|          8|    2|       0|                 1|
|1994.7749|          6|    3|       0|                 1|
|2004.3507|          8|    0|       0|                 1|
|2004.3507|          5|    2|       0|                 1|
|2004.3507|          8|    2|       0|                 1|
|2004.3507|          0|    0|       0|                 1|
|2004.3507|          0|    1|       0|                 1|
|2004.3507|          8|    2|       0|                 1|
| 2009.773|          8|    2|       0|                 1|
|2030.7837|          4|    0|       0|                 1|
| 2034.993|          6|    2|       0|                 1|
|2050.6235|          8|    1|       0|                 1|
| 2052.949|          8|    3|       0|                 1|
| 2063.388|          8|    2|       0|                 1|
|2064.4587|          8|    0|       0|                 1|
| 2064.698|          4|    1|       0|                 1|
+---------+-----------+-----+--------+------------------+
only showing top 20 rows
*/

/*
-----------------------CONCLUSIONS-----------------------
-> Agents were the most popular sales channel
-> The responses were mostly negative, but the highest positive rate was through agents
-> Most customers used Personal L3 policy and least used is special L1
-> Most customers chose 1 Policy
-> Basic Coverage was mostly subscribed
-> The people with higher CLV have subscribed mostly to Premium, basic becomes more scarce
-> 2081 customers opted for the best policy and coverage plan, 
    only 38 people opted for the most popular coverage and least opted policy type
-> As high as 3251 customers have enrolled in only one policy plan, and as less as 372 people 
    opted for 6 policies. There does not seem to be any direct relationship between the number of policies 
    and total no of customers as about 416 customers have subscribed to about 9 policy plans
-> Most of the custtomers with high CLV have opted for either one of the 3 Personal policy types and Basic coverage type
-> We see there is no direct relationship betweenbCLV and no of policies
*/