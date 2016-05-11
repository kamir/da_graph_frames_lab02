#
# Listing 2
# =========


val e1 = sqlContext.sql("FROM yarn_ds1_run_3_webpage_parquet SELECT concat(mt,mp) as source, baseurl as target, '1' as type, explode(inlinks) as (mt,mp)")

val e2 = sqlContext.sql("FROM yarn_ds1_run_3_webpage_parquet SELECT baseurl as source, concat(mt,mp) as target, '2' as type, explode(outlinks) as (mt,mp)")

val e1c = e1.select("source","target","type")

val e2c = e2.select("source","target","type")

val eAll = e1c.unionAll( e2c )

eAll.limit(10).show()

println( eAll.count() )

val nAll1 = eAll.select("source").distinct().withColumnRenamed("source","name")

val nAll2 = eAll.select("target").distinct().withColumnRenamed("target","name")

val nAll = nAll1.unionAll( nAll2 ).distinct()

val nodes = nAll.withColumn("id", nAll("name") )

nodes.limit(10).show()




#
# So far so good .... but names much match !!!
#

val e2 = eAll.withColumnRenamed("source","src").withColumnRenamed("target","dst")

val g1 = GraphFrame(nodes, eAll)



#
# HERE we have a GraphFrame and repeat our previous analysis ...
#

