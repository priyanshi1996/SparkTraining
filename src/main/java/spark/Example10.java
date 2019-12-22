package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Example10 {
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.WARN);
		SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaRDD<String> rdd = jsc.textFile("in/Sample2.txt");
		JavaRDD<String> rdd2 = jsc.textFile("in/Sample3.txt");
		
		//Example 11: DataFile: Sample4.txt
		JavaRDD<String> distinct_rdd =rdd.distinct(); 
		System.out.println("Distinct \n " );
		for (String string : distinct_rdd.collect()) { 
			System.out.println(string);
		}
	
		JavaRDD<String> union_rdd=rdd.union(rdd2); 
		System.out.println("Union \n");
		for (String string : union_rdd.collect()) { 
			System.out.println(string);
		}
	
		JavaRDD<String> intersection_rdd=rdd.intersection(rdd2); 
		System.out.println("Intersection \n");
		for (String string :intersection_rdd.collect()) { 
			System.out.println(string);
		}
	
		JavaRDD<String> substract_rdd=rdd.subtract(rdd2); 
		System.out.println("Subtract \n");
		for (String string : substract_rdd.collect()) {
			System.out.println(string);
		}
		JavaPairRDD<String,String> cartesian_rdd=rdd.cartesian(rdd2);
		System.out.println("Cartesian \n");
		for (Tuple2<String,String> string : cartesian_rdd.collect()) {
			System.out.println(string._1+"-----"+string._2);
		}
		jsc.close(); 
	}
}
