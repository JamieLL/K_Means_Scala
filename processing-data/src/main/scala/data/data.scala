package data

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import org.apache.spark.ml.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils



object dataMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ndata.dataMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("data")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { 
        hdfs.delete(new org.apache.hadoop.fs.Path(args(0)), true)
    } catch { case _: Throwable => {} }
    // ================

    var flight = spark.read.format("csv").option("header","true").load(args(1))
    val colNames = Seq("DayofYear","DayOfWeek","CRSDepTime","CRSArrTime","DepTime","ArrTime","CRSElapsedTime","ArrDelay","DepDelay","AirTime","Distance",
"TaxiIn","TaxiOut") // total 13 cols
    flight = flight.select(colNames.map(c => col(c)): _*)

    val scaler1 = new StandardScaler(withMean = true, withStd = true).fit(flight.map(x => x.features))
    val scaler2 = new SrandardScalerModel(scaler1.std, scaler1.mean)
    val data1 = flight.map(x => (x.label,scaler2.transform(Vectors.dense(x.features.toArray)))

    val scaler = new StandardScaler()
			.setInputCol("")
			.setOutputCol("scaled")
			.setWithStd(true)
			.setWithMean(false)
    val pipeline = new Pipeline().setStages(Array(scaler))
    val scaleModel = pipeline.fit(flight)
    val scaledData = scalerModel.transform(flight)

    // for i in colNames:
	// assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")
        // scaler = MinMaxScaler(inputCol=i+"_Vect",outputCol=i+"_Scaled")
	// pipeline = Pipeline(stages=[assembler,scaler])
	// flight = pipeline.fit(flight).transform(flight).withColumn(i+"_Scaled",unlist(i+"_Scaled")).drop(i+"_Vect")


  }
}
