package spark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Example18 {
	public static void main(String[] args) {
		Example18 app = new Example18();
		app.start();
	}

	private void start() { 
		Logger.getLogger("org").setLevel(Level.ERROR); 
		SparkSession spark = SparkSession.builder() //
				.appName("CSV to DB") .master("local") .getOrCreate();
		Dataset<Row> df = spark.read() 
				.format("csv")
				.option("header", "true") 
				.load("in/authors.csv");
		df = df.withColumn(  "name",concat(df.col("lname"), lit(", "), df.col("fname"))); //
		String dbConnectionUrl = "jdbc:postgresql://localhost/course_data";
		// We should ensure that the database is created. 
		Properties prop = new Properties(); 
		prop.setProperty("driver", "org.postgresql.Driver"); 
		prop.setProperty("user", "postgres"); 
		prop.setProperty("password", "priya123");
		df.write() //
		.mode(SaveMode.Overwrite).jdbc(dbConnectionUrl, "project2", prop);
		System.out.println("Process complete"); 
	}
}
