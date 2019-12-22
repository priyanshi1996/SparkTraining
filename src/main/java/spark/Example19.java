package spark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Example19 {
	public static void main(String args[]) throws InterruptedException
	{ 
		Logger.getLogger("org").setLevel(Level.ERROR);
		// Create a session
		SparkSession spark = new SparkSession.Builder()
				.appName("CSV to DB") .master("local[*]") .getOrCreate();
		// get data
		Dataset<Row> df = spark.read().format("csv")
				.option("header", true) .load("in/name_and_comments.txt");
		df.show();
		// Transformation
		df = df.withColumn("full_name",concat(df.col("last_name"), lit(", "), df.col("first_name"))) 
				.filter(df.col("comment").rlike("\\d+"))
				.orderBy(df.col("last_name").asc());
		//we are first doing a concat function for full name and adding a literal //with , so that it will come as a fourth column
		// Then we are doing a filter with a rlike function.
		// Then we do a orderBy on the last_name col with ascending option. //DataSets are immutable
		// Write to destination
		// df.show();
		String dbConnectionUrl = "jdbc:postgresql://localhost/course_data"; // <<- You need to create this database as shown earlier.
		Properties prop = new Properties();
		prop.setProperty("driver", "org.postgresql.Driver");
		prop.setProperty("user", "postgres");
		prop.setProperty("password", "priya123"); // <- The password you used while
		df.write()
		.mode(SaveMode.Overwrite) .jdbc(dbConnectionUrl, "project1", prop);
	}
}
