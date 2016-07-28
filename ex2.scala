import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.graphframes._

//
// We start loading the raw data from a parquet file and register it as a temporary table 
//
val crawlDataFrame = sqlContext.read.parquet("yarn_ds1_run_3_webpage_parquet_1k")
crawlDataFrame.registerTempTable("yarn_ds1_run_3_webpage_parquet")

//
// Here we assume that we have a Hive table named yarn_ds1_run_3_webpage_parquet 
// 
//    *** FIX THE ORDER ***
// 
val e1 = sqlContext.sql("FROM yarn_ds1_run_3_webpage_parquet SELECT concat(mt,mp) as source, baseurl as target, '1' as type, explode(inlinks) as (mt,mp)")
val e2 = sqlContext.sql("FROM yarn_ds1_run_3_webpage_parquet SELECT baseurl as source, concat(mt,mp) as target, '2' as type, explode(outlinks) as (mt,mp)")

val e1c = e1.select("source","target","type")
val e2c = e2.select("source","target","type")
val eAll = e1c.unionAll( e2c )
eAll.limit(10).show()

val nAll1 = eAll.select("source").distinct().withColumnRenamed("source","name")
val nAll2 = eAll.select("target").distinct().withColumnRenamed("target","name")
val nAll = nAll1.unionAll( nAll2 ).distinct()

val nodes = nAll.withColumn("id", nAll("name") )
nodes.limit(10).show()

val e2 = eAll.withColumnRenamed("source","src").withColumnRenamed("target","dst")

val g1 = GraphFrame(nodes, e2)

val k = g1.degrees.sort(desc("degree"))
k.show()

val pr2 = g1.pageRank.resetProbability(0.15).maxIter(10).run()

val pr3 = pr2.vertices.sort(desc("pagerank"))

val tcr = g1.triangleCount.run()
tcr.sort(desc("count")).show()

tcr.printSchema
pr2.vertices.printSchema

val scatter = pr2.vertices.join(tcr, pr2.vertices.col("id").equalTo(tcr("id")))

scatter.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("scatter_2.csv")
