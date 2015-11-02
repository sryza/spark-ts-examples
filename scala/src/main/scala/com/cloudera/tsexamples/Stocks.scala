import com.cloudera.sparkts._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object Stocks {
  /**
   * Creates a Spark DataFrame of (timestamp, symbol, price) from a tab-separated file of stock
   * ticker data.
   */
  def loadTickerObservations(sqlCtx: SQLContext, path: String): DataFrame = {
    val rowRdd = sqlCtx.sparkContext.textFile(path).map { line =>
      val tokens = line.split('\t')
      val dt = new DateTime(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, tokens(3).toInt)
      val symbol = tokens(4)
      val price = tokens(6).toDouble
      Row(dt, symbol, price)
    }
    val fields = Seq(
      StructField("timestamp", new TimestampType(), true),
      StructField("symbol", new StringType(), true),
      StructField("price", new DoubleType(), true)
    )
    val schema = StructType(fields)
    sqlCtx.createDataFrame(rowRdd, schema)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark-TS Stocks Example")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)

    val tickerObs = loadTickerObservations(sqlCtx, "../data/ticker.tsv")

    val dtIndex = DateTimeIndex.uniform(
      new DateTime("2015-08-03"), new DateTime("2015-09-22"), new HourFrequency(1))
    val tickerTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, tickerObs,
      "timestamp", "symbol", "price")
  }
}
