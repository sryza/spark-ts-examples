package com.cloudera.tsexamples;

public class JavaSingleSeriesARIMA {
public static void main(String[] args) throws NumberFormatException, IOException
	{
		String filename = "../spark-ts-examples/data/R_ARIMA_DataSet1.csv";
		SparkConf conf = new SparkConf().setAppName("Spark Time Series");
		JavaSparkContext sc = new JavaSparkContext(conf); 
		
		JavaRDD<String> ARIMA_DataSet1 = sc.textFile(filename);
		JavaRDD<Double> data_points = ARIMA_DataSet1.map(new Function<String, Double>() {

			@Override
			public Double call(String line) throws Exception {
				// TODO Auto-generated method stub
				Double val = Double.parseDouble(line);
				return val;
			}
			
		});

		List<Double> doubleList = data_points.collect();

		Double[] doubleArray = new Double[doubleList.size()];
		doubleArray = doubleList.toArray(doubleArray);

		double[] values = new double[doubleArray.length];
		for (int i = 0; i < doubleArray.length; i++){
			values[i] = doubleArray[i];
		}

		Vector tsVector = Vectors.dense(values);
		System.out.println("TS vector:" + tsVector.toString());

		ARIMAModel arimaModel = ARIMA.autoFit(tsVector, 1, 0, 1);

		System.out.println("*** ARIMA Model Coefficients ***");
		for (double coefficient : arimaModel.coefficients()){
			System.out.println("ARIMA model coefficient:" + coefficient);

		}

		Vector forecast = arimaModel.forecast(tsVector, 10);
		System.out.println("ARIMA model forecast for next 10 observations:" + forecast);
	}
}
