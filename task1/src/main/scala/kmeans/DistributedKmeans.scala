package kmeans

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import scala.util.control.Breaks._
import scala.math
import org.apache.spark.sql.SQLContext


object DistributedKmeansMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nkmeans.DistributedKmeansMain <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Distributed K means")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate()

    //------------------function part--------------------
    //calculate Euclidean distance between two points
    def calculateDist(data:Seq[Double], centerPoint:Seq[Double]): Double = {
      val n = centerPoint.length
      var distance: Double = 0.0

      for (i <- 0 to n - 1){
        //for each dimension, calculate distance
        val tmp_dist = math.pow(data(i) - centerPoint(i), 2)
        distance += tmp_dist
      }
      return math.sqrt(distance)
    }

    //write a function to find the closest centroid
    def nearestCenter(data: Seq[Double], centroid: Array[Row]): Int = {
      var targetCluster = 0
      var min_dist = Double.MaxValue
      val n = centroid.length

      //loop through all centroids, check given data point is close to which center
      for (i <- 0 to n - 1){
        val tmp_dist = calculateDist(data, centroid(i).toSeq.asInstanceOf[Seq[Double]])
        if (tmp_dist < min_dist){
          targetCluster = i
          min_dist = tmp_dist
        }
      }
      return targetCluster
    }

    //calculate total distance between new and old centers, used for converge
    def calculateTotalDistance(newCenter: Array[Seq[Double]], oldCenter: Array[Row]): Double = {
      val n = newCenter.length
      var cumulative_sum = 0.0
      for (i <- 0 to n - 1){
          val tmp_distance = calculateDist(newCenter(i), oldCenter(i).toSeq.asInstanceOf[Seq[Double]])
          cumulative_sum += tmp_distance
      }
      return cumulative_sum
    }

    //------------------main program--------------------
    //define parameters
    //threshold is used to check whether the result is converge
    val threshold = 0.001
    //k is the number of clusters
    val k = 25

    //read from csv
    var df = spark.read.option("header", true).csv(args(0))
    
    //change all columns to double
    val colNames = Seq("DayofYear","DayOfWeek","CRSDepTime","CRSArrTime","DepTime","ArrTime","CRSElapsedTime","ArrDelay","DepDelay","AirTime","Distance",
      "TaxiIn","TaxiOut")
    val myschema = StructType(colNames.map(field => StructField(field, DoubleType,true)))
    val toDouble = udf[Double,String](_.toDouble)
    for (i <- 0 to colNames.size - 1){
      df = df.withColumn(colNames(i), toDouble(df(colNames(i))))
    }

    //initialize k centroidsd by randomly select k points from dataset
    var centroid = df.sample(false, 0.5).limit(k)

    //main part of k means clustering
    //iteration begins, set a break condition if the result is converge
    breakable {
      for (i <- 1 to 200) {

        //broadcast centroids to all machines
        val centerlist = broadcast(centroid).collect()

        //assgin data points based on current k centers
        val new_assignment = df.rdd.map {
              //store as an rdd in which key is the cluster id, value is the coordinates
          row => {
            val rowSeq = row.toSeq.asInstanceOf[Seq[Double]]
            val cluster_idx = nearestCenter(rowSeq, centerlist)
            (cluster_idx, rowSeq)
          }
        }

        //get new center by taking the average of those points in the cluster
        //change value to (value, 1) in order to count number of points inside each cluster
        val new_center_coord = new_assignment.mapValues(value => (value, 1)).reduceByKey {
          case ((p1, c1), (p2, c2)) =>
            val res = Seq[Double](p1(0) + p2(0), p1(1) + p2(1), p1(2) + p2(2), p1(3) + p2(3), p1(4) + p2(4), p1(5) + p2(5), p1(6) + p2(6), p1(7) + p2(7), p1(8) + p2(8), p1(9) + p2(9), p1(10) + p2(10), p1(11) + p2(11), p1(12) + p2(12))
            (res, c1 + c2)
        //sort by key to guarantee that further update step works good
        }.sortBy(_._1).map {
          case (cluster, (p1, c1)) =>
            val res = Seq[Double](p1(0) / c1, p1(1) / c1, p1(2) / c1, p1(3) / c1, p1(4) / c1, p1(5) / c1, p1(6) / c1, p1(7) / c1, p1(8) / c1, p1(9) / c1, p1(10) / c1, p1(11) / c1, p1(12) / c1)
            (cluster, res)
        }

        val new_center = new_center_coord.map(x => x._2)
        //check convergence
        val total_sum_distance = calculateTotalDistance(new_center.collect(), centerlist)
        if (total_sum_distance < threshold || i == 200) {

          //calculate SSE(sum of squared error)
          val sse = new_assignment.join(new_center_coord).mapValues{
            case (center, data) => calculateDist(center, data)
          }.map(_._2).reduce(_ + _)

          println("Loop stop here, when i = " + i.toString)
          println("Sum of squared when k = " + k.toString + " , is " + sse.toString)
            break
        }

        //convert rdd back to dataframe and update centroid
        centroid = spark.createDataFrame(new_center.map(Row.fromSeq(_)), myschema)
      }
    }
    println("Debug String is here")
    logger.info(centroid.rdd.toDebugString)
    centroid.rdd.saveAsTextFile(args(1))

}
}
