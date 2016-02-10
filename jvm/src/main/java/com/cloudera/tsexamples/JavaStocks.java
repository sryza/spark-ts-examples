package com.cloudera.tsexamples;

import breeze.linalg.DenseVector;
import com.cloudera.sparkts.BusinessDayFrequency;
import com.cloudera.sparkts.DateTimeIndex;
import com.cloudera.sparkts.api.java.DateTimeIndexFactory;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDD;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDDFactory;
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class JavaStocks {
  private static DataFrame loadObservations(JavaSparkContext sparkContext, SQLContext sqlContext,
      String path) {
    JavaRDD<Row> rowRdd = sparkContext.textFile(path).map(new Function<String, Row>() {
      public Row call(String line) {
        String[] tokens = line.split("\t");
        ZonedDateTime dt = ZonedDateTime.of(Integer.parseInt(tokens[0]),
            Integer.parseInt(tokens[1]), Integer.parseInt(tokens[1]), 0, 0, 0, 0,
            ZoneId.systemDefault());
        String symbol = tokens[3];
        double price = Double.parseDouble(tokens[4]);
        return RowFactory.create(Timestamp.from(dt.toInstant()), symbol, price);
      }
    });
    List<StructField> fields = new ArrayList();
    fields.add(DataTypes.createStructField("timestamp", DataTypes.TimestampType, true));
    fields.add(DataTypes.createStructField("symbol", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("price", DataTypes.DoubleType, true));
    StructType schema = DataTypes.createStructType(fields);
    return sqlContext.createDataFrame(rowRdd, schema);
  }

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Spark-TS Ticker Example").setMaster("local");
    conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");
    JavaSparkContext context = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(context);

    DataFrame tickerObs = loadObservations(context, sqlContext, "../data/ticker.tsv");

    // Create an daily DateTimeIndex over August and September 2015
    DateTimeIndex dtIndex = DateTimeIndexFactory.uniformFromInterval(
        ZonedDateTime.parse("2015-08-03"), ZonedDateTime.parse("2015-09-22"),
        new BusinessDayFrequency(1, 0));

    // Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD
    JavaTimeSeriesRDD tickerTsrdd = JavaTimeSeriesRDDFactory.javaTimeSeriesRDDFromObservations(
        dtIndex, tickerObs, "timestamp", "symbol", "price");

    // Cache it in memory
    tickerTsrdd.cache();

    // Count the number of series (number of symbols)
    System.out.println(tickerTsrdd.count());

    // Impute missing values using linear interpolation
    JavaTimeSeriesRDD<String> filled = tickerTsrdd.fill("linear");

    // Compute return rates
    JavaTimeSeriesRDD<String> returnRates = filled.returnRates();

    // Compute Durbin-Watson stats for each series
    JavaPairRDD<String, Double> dwStats = returnRates.mapValues(new Function<Vector, Double>() {
      public Double call(Vector x) {
        TimeSeriesStatisticalTests.dwtest(new DenseVector<Double>(x.toArray()))
      }
    });

    System.out.println(dwStats.map(_.swap).min)
    System.out.println(dwStats.map(_.swap).max)
  }
}

