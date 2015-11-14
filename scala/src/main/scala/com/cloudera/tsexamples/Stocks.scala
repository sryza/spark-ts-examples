package com.cloudera.tsexamples

import java.sql.Timestamp

import com.cloudera.sparkts._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import com.cloudera.sparkts.models.Autoregression

object Stocks {
  /**
   * Creates a Spark DataFrame of (timestamp, symbol, price) from a tab-separated file of stock
   * ticker data.
   */
  def loadTickerObservations(sqlContext: SQLContext, path: String): DataFrame = {
    val rowRdd = sqlContext.sparkContext.textFile(path).map { line =>
      val tokens = line.split('\t')
      val dt = new DateTime(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, tokens(3).toInt, 0)
      val symbol = tokens(4)
      val price = tokens(6).toDouble
      Row(new Timestamp(dt.getMillis), symbol, price)
    }
    val fields = Seq(
      StructField("timestamp", TimestampType, true),
      StructField("symbol", StringType, true),
      StructField("price", DoubleType, true)
    )
    val schema = StructType(fields)
    sqlContext.createDataFrame(rowRdd, schema)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark-TS Stocks Example").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val tickerObs = loadTickerObservations(sqlContext, "../data/ticker.tsv")

    // Create an hourly DateTimeIndex over August and September 2015
    val dtIndex = DateTimeIndex.uniform(
      new DateTime("2015-08-03"), new DateTime("2015-09-22"), new DayFrequency(1))

    // Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD
    val tickerTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, tickerObs,
      "timestamp", "symbol", "price")

    // Count the number of series (number of symbols)
    println(tickerTsrdd.count())

    // Impute missing values using linear interpolation
    val filled = tickerTsrdd.fill("linear")

    // Compute return rates
    val returnRates = filled.returnRates()

    // Fit an autoregressive model to each series
    val models = returnRates.mapValues(Autoregression.fitModel(_, 1))

    // Which has the largest AR coefficient?
    val symbolWithLargestPhi = models.mapValues(_.coefficients(0)).map(_.swap).max
    println(symbolWithLargestPhi)
  }
}
