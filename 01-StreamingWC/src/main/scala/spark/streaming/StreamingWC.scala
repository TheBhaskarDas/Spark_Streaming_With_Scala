package spark.streaming

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingWC extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Streaming Word Count")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()
    println(spark.version)
    println(spark.sparkContext.version)


    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    // linesDF.printSchema()

    //val wordsDF = linesDF.select(explode(split(col("value"), " ")).alias("word"))
    val wordsDF = linesDF.select(expr("explode(split(value,' ')) as word")) //The Whole things are an expression so we must enclose inside expr.
    val countsDF = wordsDF.groupBy("word").count()
    //Explode Function Transforms Array of Words into Rows. So Here My Texts(Which are coming through TCP/IP call) are broken into Words and Exploded into Rows.

    val wordCountQuery = countsDF.writeStream
      .format("console")
      //.option("numRows", 2)
      .outputMode("complete")
      .option("checkpointLocation", "chk-point-dir")
      .start()

    logger.info("Listening to localhost:9999")
    wordCountQuery.awaitTermination()


  }

}
