import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.graphframes._

val customSchema = StructType(Array(StructField("src", IntegerType, true),StructField("dst", IntegerType, true)))

val edges = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", " ").schema(customSchema).load("facebook_combined.txt")

val n1 = edges.select("src").distinct()
val n2 = edges.select("dst").distinct()
val n = n1.unionAll( n2 ).withColumnRenamed("src","name").distinct()

val nodes = n.withColumn("id", n("name") )

val g1 = GraphFrame(nodes, edges)

val k = g1.degrees.sort(desc("degree"))

val pr2 = g1.pageRank.resetProbability(0.15).maxIter(10).run()
<<<<<<< HEAD
=======

pr2.vertices.show()
>>>>>>> f7e39b7b1a2b68e482ba079262aa2f1a830fbed4

val pr3 = pr2.vertices.sort(desc("pagerank"))

val tcr = g1.triangleCount.run()

tcr.printSchema
pr2.vertices.printSchema

val scatter = pr2.vertices.join(tcr, pr2.vertices.col("id").equalTo(tcr("id")))

scatter.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("scatter_1.csv")


<<<<<<< HEAD
=======
scatter.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("scatter_1.csv")
>>>>>>> f7e39b7b1a2b68e482ba079262aa2f1a830fbed4
