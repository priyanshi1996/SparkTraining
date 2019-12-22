package spark;

import java.util.ArrayList; 
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext; 
public class Example6 {
	
	public static void main(String[] args) {
		List<Integer> inputData = new ArrayList<>(); 
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20); 
		Logger.getLogger("org").setLevel(Level.WARN); 
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> myRdd = sc.parallelize(inputData);
		// 1] This is to call the reduce function
		Integer result = myRdd.reduce((value1, value2 ) -> value1 + value2 );
		// 2] This is find the sqrt of the values in the RDD
		JavaRDD<Double> sqrtRdd = myRdd.map( value -> Math.sqrt(value) );
		sqrtRdd.foreach( value -> System.out.println(value));
		// sqrtRdd.foreach( System.out::println );
		// This is Java 8 way of doing it
		// Incase if you have a multiple core there you might an exception called
		// NotSerializableException
		sqrtRdd.collect().forEach( System.out::println );
		// 3] How many elements in sqrtRdd System.out.println(sqrtRdd.count()); // using just map and reduce
		JavaRDD<Long> singleIntegerRdd = sqrtRdd.map( value -> 1L);
		Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
		System.out.println(count);
		System.out.println(result);
		sc.close(); 
	}
} 