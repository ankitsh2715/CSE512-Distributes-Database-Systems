package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {

    // Load the original data from a data source
    var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.createOrReplaceTempView("pickupInfoView")
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    pickupInfo = spark.sql(s"select x,y,z from pickupInfoView where x>= ${minX} and x<= ${maxX} and y>= ${minY} and y<= ${maxY} and z>= ${minZ} and z<= ${maxZ}")
    pickupInfo = pickupInfo.groupBy("x", "y", "z").count()
    
    val avg: Double = pickupInfo.agg(sum("count") / numCells).first.getDouble(0)
    val sdeviation: Double = math.sqrt(pickupInfo.agg(sum(pow("count", 2.0)) / numCells - math.pow(avg, 2.0)).first.getDouble(0))
    
    pickupInfo.createOrReplaceTempView("hotcells")
    
    spark.udf.register("ST_Within", (x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int) => (HotcellUtils.ST_Within(x1, y1, z1, x2, y2, z2)))
    var joinTable = spark.sql("select hc1.x as x, hc1.y as y, hc1.z as z, hc2.count as count, 1 as neighbour from hotcells hc1, hotcells hc2 where ST_Within(hc1.x, hc1.y, hc1.z, hc2.x, hc2.y, hc2.z)")
    joinTable = joinTable.groupBy("x", "y", "z").sum("count","neighbour")
    joinTable = joinTable.toDF(Seq("x", "y", "z", "sum", "weight"): _*)
    joinTable.createOrReplaceTempView("selfJoinTbl")

    spark.udf.register("gScore", (sum: Int, weight:Int) => HotcellUtils.gScore(avg, sdeviation, numCells, sum, weight))
    var res = spark.sql("select x, y, z, gScore(sum, weight) as score from selfJoinTbl")
      
    return res.sort(desc("score")).limit(50).select("x","y","z")
  }
}