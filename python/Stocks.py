from datetime import datetime

from pyspark import SparkContext, SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, StringType

from sparkts.datetimeindex import uniform, BusinessDayFrequency
from sparkts.timeseriesrdd import time_series_rdd_from_observations

def lineToRow(line):
    (year, month, day, symbol, volume, price) = line.split("\t")
    # Python 2.x compatible timestamp generation
    dt = datetime(int(year), int(month), int(day))
    return (dt, symbol, float(price))

def loadObservations(sparkContext, sqlContext, path):
    textFile = sparkContext.textFile(path)
    rowRdd = textFile.map(lineToRow)
    schema = StructType([
        StructField('timestamp', TimestampType(), nullable=True),
        StructField('symbol', StringType(), nullable=True),
        StructField('price', DoubleType(), nullable=True),
    ])
    return sqlContext.createDataFrame(rowRdd, schema);

if __name__ == "__main__":
    sc = SparkContext(appName="Stocks")
    sqlContext = SQLContext(sc)

    tickerObs = loadObservations(sc, sqlContext, "../data/ticker.tsv")
    
    # Create an daily DateTimeIndex over August and September 2015
    freq = BusinessDayFrequency(1, 1, sc)
    dtIndex = uniform(start='2015-08-03T00:00-07:00', end='2015-09-22T00:00-07:00', freq=freq, sc=sc)
    
    # Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD
    tickerTsrdd = time_series_rdd_from_observations(dtIndex, tickerObs, "timestamp", "symbol", "price")

    # Cache it in memory
    tickerTsrdd.cache()
    
    # Count the number of series (number of symbols)
    print(tickerTsrdd.count())
    
    # Impute missing values using linear interpolation
    filled = tickerTsrdd.fill("linear")
    
    # Compute return rates
    returnRates = filled.return_rates()
    
    # Durbin-Watson test for serial correlation, ported from TimeSeriesStatisticalTests.scala
    def dwtest(residuals):
        residsSum = residuals[0] * residuals[0]
        diffsSum = 0.0
        i = 1
        while i < len(residuals):
            residsSum += residuals[i] * residuals[i]
            diff = residuals[i] - residuals[i - 1]
            diffsSum += diff * diff
            i += 1
        return diffsSum / residsSum
    
    # Compute Durbin-Watson stats for each series
    # Swap ticker symbol and stats so min and max compare the statistic value, not the
    # ticker names.
    dwStats = returnRates.map_series(lambda row: (row[0], [dwtest(row[1])])).map(lambda x: (x[1], x[0]))
    
    print(dwStats.min())
    print(dwStats.max())
