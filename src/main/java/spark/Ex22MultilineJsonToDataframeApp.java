package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

//MultilineJsonToDataframeApp
public class Ex22MultilineJsonToDataframeApp {

	public static void main(String[] args) {
		Ex22MultilineJsonToDataframeApp app = new Ex22MultilineJsonToDataframeApp();
		app.start();
	}

	private void start() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkSession spark = SparkSession.builder()
				.appName("Multiline JSON to Dataframe")
				.master("local")
				.getOrCreate();
		Dataset<Row> df = spark.read().format("json")
				.option("multiline", true) // Most Imp function
				.load("in/countrytravelinfo.json");
		df.show(3);
		df.printSchema();
	}
}