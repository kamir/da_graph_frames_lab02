import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.graphframes._

val customSchema = StructType(Array(StructField("src", IntegerType, true),StructField("dst", IntegerType, true)))

val edges = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", " ").schema(customSchema).load("facebook_combined.txt")

df.show()

val n1 = edges.select("src").distinct()
val n2 = edges.select("dst").distinct()
val n = n1.unionAll( n2 ).withColumnRenamed("src","name")
val nodes = n.withColumn("id", n("name") )

edges.show()
nodes.show()

val g1 = GraphFrame(nodes, edges)




