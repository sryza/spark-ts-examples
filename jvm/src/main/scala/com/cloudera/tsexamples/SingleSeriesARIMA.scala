package com.cloudera.tsexamples

import com.cloudera.sparkts.models.ARIMA
import breeze.linalg.DenseVector

/**
 * An example showcasing the use of ARIMA in a non-distributed context.
 */
object SingleSeriesARIMA {
  def main(args: Array[String]): Unit = {
    // The dataset is sampled from an ARIMA(1, 0, 1) model generated in R.
    val lines = scala.io.Source.fromFile("../data/R_ARIMA_DataSet1.csv").getLines()
    val ts = new DenseVector[Double](lines.map(_.toDouble).toArray)
    val arimaModel = ARIMA.fitModel(1, 0, 1, ts)
    println("coefficients: " + arimaModel.coefficients)
    val forecast = arimaModel.forecast(ts, 20)
    println("forecast of next 20 observations: " + forecast.toArray.mkString(","))
  }
}
