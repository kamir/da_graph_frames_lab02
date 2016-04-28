import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.graphframes._

val customSchema = StructType(Array(StructField("src", IntegerType, true),StructField("dst", IntegerType, true)))

val edges = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", " ").schema(customSchema).load("facebook_combined.txt")

val n1 = edges.select("src").distinct()
val n2 = edges.select("dst").distinct()
val n = n1.unionAll( n2 ).withColumnRenamed("src","name")
val nodes = n.withColumn("id", n("name") )

edges.show()
nodes.show()

val g1 = GraphFrame(nodes, edges)

val k = g1.degrees.sort(desc("degree"))
k.show()

// val pr1 = g1.pageRank.resetProbability(0.15).tol(0.01).run()
// pr.vertices.show()

val pr2 = g1.pageRank.resetProbability(0.15).maxIter(10).run()
pr2.vertices.show()

val tcr = g1.triangleCount.run()
tcr.sort(desc("count").show()




