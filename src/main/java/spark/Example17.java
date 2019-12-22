package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Example17 {
	public static void main(String[] args) {
		Example17 app = new Example17();
		app.start();
	}

	private void start() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkSession spark = SparkSession.builder().appName("CSV to Dataset").master("local").getOrCreate();
		Dataset<Row> df = spark.read().format("csv").option("header", "true").load("in/books.csv");
		df.show(5);
	}
}
