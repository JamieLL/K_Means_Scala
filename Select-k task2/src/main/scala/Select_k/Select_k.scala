package Select_k

import java.io.{File, PrintWriter}

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.broadcast

import scala.collection.mutable.ListBuffer
object Select_k {
  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate();

    //Read in the data & do the k-means clustering
    val data: DataFrame = spark.read.format("csv").option("header","true")
      .load("full2m.csv")

    val pw = new PrintWriter(new File("all_center.txt"))
    val num_data = data.count()
    /*val textFile = sc.textFile(args(0))
    val getCenter = textFile.map{
      case(k)=>{
        (k, data.sample(withReplacement = false, k.toInt/num_data).limit(k.toInt).collect())
      }
    }*/

    //Determine how many different configurations to have
   // val centroid: Array[Row] = data.sample(withReplacement = false, 0.1).limit(2).collect()



    val kList = List(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
    for (i <- kList.indices) {
      pw.write(kList(i).toString)
      pw.write("@")
      val centroid: Array[Row] = data.sample(withReplacement = false, 0.1).limit(2).collect()
      for (k <- centroid.indices) {
          val row: Row = centroid(k)
          for (j <- 0 to row.length - 1) {
            pw.write("" + row.get(j))
            if (j == (row.length - 1)) {
              if (k != (centroid.length - 1)) {
                pw.write("~")
              }
            } else {
              pw.write(",")
            }
          }
      }
      pw.write("\n")
    }

    pw.close()

    //getCenter.saveAsTextFile(args(1))
  }



  }
