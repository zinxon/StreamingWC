import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object StreamingWC extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Streaming Word Count")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.streaming.stopGracefullyOnShutDown", value = true)
      .config("spark.sql.shuffle.partitions", value = 3)
      .getOrCreate()

    val linesDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    //    linesDF.printSchema()
    val wordDF = linesDF.select(expr("explode(split(value,' ')) as word"))
    val countsDF = wordDF.groupBy("word").count()

    val wordCountQuery = countsDF.writeStream
      .format("console")
      .option("checkpointLocation", "chk-point-dir")
      .outputMode("complete")
      .start()
    wordCountQuery.awaitTermination()
  }
}

