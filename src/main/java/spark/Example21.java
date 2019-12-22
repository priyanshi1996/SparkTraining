package spark;

import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class Example21 {
	public static void main(String[] args) {
		Example21 app = new Example21();
		app.start();
	}

	private void start() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkSession spark = SparkSession.builder()
				.appName("Array to Dataset<String>")
				.master("local")
				.getOrCreate();
		String[] stringList = new String[] { "Jean", "Liz", "Pierre", "Lauric" };
		List<String> data = Arrays.asList(stringList);
		Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
		ds.show();
		ds.printSchema();
	}
}
