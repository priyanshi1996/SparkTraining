package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ex22XmlToDataframeApp {
	public static void main(String[] args) {
		Ex22XmlToDataframeApp app = new Ex22XmlToDataframeApp();
		app.start();
	}

	private void start() { 
		Logger.getLogger("org").setLevel(Level.ERROR); 
		SparkSession spark = SparkSession.builder()
				.appName("XML to Dataframe") .master("local")
				.getOrCreate();
		Dataset<Row> df = spark.read().format("xml")  
				.option("rowTag", "row")// Element or tag that indicates a record in the XML file 
				.load("in/NASA_Patents.xml");
		df.show(5);
		df.printSchema(); 
	}
}
