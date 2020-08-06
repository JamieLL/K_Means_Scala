package localkmeans

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.broadcast
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

object LocalKMeans {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate();

    // Read in k & get the centers
    val textFile = sc.textFile(args(0) +"/all_center.txt", minPartitions=10)
    val getCenter = textFile.map(line => (line.split("@")(0), line.split("@")(1).split("~")))
      .map {
        case (k,v) => {
          var allKList= new ListBuffer[List[Double]]()
          v.flatMap { l =>
            var klist = new ListBuffer[Double]()
            val tmpList: Array[String] = l.split(",")
            for (k <- tmpList.indices) {
              klist += tmpList(k).toDouble
            }
            allKList += klist.toList
          }
          (k,allKList.toList)
        }
      }

    //Read in the data & do the k-means clustering
    val data: DataFrame = spark.read.format("csv").option("header","true")
      .load(args(0) + "/sample_after.csv")
    val tmpData: Array[Row] = broadcast(data.as("newData")).collect()

    def dataStringToDouble(input:Row) ={
      var tmp= List.fill(input.length)(0.0)
      for (i<-tmp.indices) {
        val t = input.toSeq.apply(i).asInstanceOf[String].toDouble
        tmp = tmp.updated(i, t)
      }
      tmp
    }

    // calculate the distance between point and one center
    def calculateDist(data:List[Double], centerPoint:List[Double]): Double = {
      val n = centerPoint.length
      var distance: Double = 0.0
      for (i <- 0 until n - 1){
        //for each dimension, calculate distance
        val tmp_dist = math.pow(data(i) - centerPoint(i), 2)
        distance += tmp_dist
      }
      math.sqrt(distance)
    }

    //local kmeans
    def kmeans(klist:List[List[Double]], data: Array[Row], iter:Int) : List[List[Double]] = {
      var kOriginal = klist
      val loop = new Breaks
      import loop.{break,breakable}

      breakable{
        for (_ <-1 to iter) {
          var kOld = kOriginal
          var countForEachK = Array.fill(kOriginal.length)(0)
          var dataForEachK = new ListBuffer[List[Double]]()
          for (i <- kOriginal.indices) {
            var oneCenter = List.fill(kOriginal(i).length)(0.0)
            dataForEachK += oneCenter
          }
          for (i <- data.indices) {
            val one_data = dataStringToDouble(data(i))

            var min_distance = Double.MaxValue
            var index = -1

            val one_data_asList = one_data.toSeq.asInstanceOf[List[Double]]
            for (i <- kOriginal.indices) {
              val new_distance = calculateDist(kOriginal(i), one_data_asList)
              if (min_distance > new_distance) {
                min_distance = new_distance
                index = i
              }
            }
            countForEachK(index) = 1 + countForEachK(index)
            for (k <- dataForEachK(index).indices) {
              dataForEachK(index) = dataForEachK(index).updated(k, dataForEachK(index)(k) + one_data_asList(k))
            }
          }
          for (i <- dataForEachK.indices) {
            var tmp = dataForEachK(i)
            for (k <- dataForEachK(i).indices) {
              tmp = tmp.updated(k, dataForEachK(i)(k) / countForEachK(i))
            }
            kOriginal = kOriginal.updated(i, tmp)
          }

          var distance_check = 0.0
          for (i<-kOld.indices){
            distance_check = distance_check+calculateDist(kOld(i),kOriginal(i))
          }
          println(distance_check)

          if(distance_check<0.001)
          {
            println("break here!")
            break
          }
        }
      }
      kOriginal
    }

    val resultCenter: RDD[(String, List[List[Double]])] = getCenter.map{
      case(k,v)=> (k,kmeans(v,tmpData,100))
    }

    /*def findBestK(klist:List[List[Double]], data: Array[Row]) : Double ={
      var sum = 0.0
      var kOriginal = klist
      for (i <- data.indices) {
        val one_data = dataStringToDouble(data(i))
        var min_distance = Double.MaxValue

        val one_data_asList = one_data.toSeq.asInstanceOf[List[Double]]
        for (i <- kOriginal.indices) {
          val new_distance = calculateDist(kOriginal(i), one_data_asList)
          if (min_distance > new_distance) {
            min_distance = new_distance
          }
        }
        sum += min_distance
      }
      sum
    }

    val resultErrors = resultCenter.map {
      case(k, v) => (k, findBestK(v, tmpData))
    }

    resultErrors.saveAsTextFile(args(1))*/

    resultCenter.saveAsTextFile(args(1))
  }
}
