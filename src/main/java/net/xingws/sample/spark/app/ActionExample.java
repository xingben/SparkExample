/**
 * 
 */
package net.xingws.sample.spark.app;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import net.xingws.sample.sample.spark.transformation.AddAndCount;
import net.xingws.sample.sample.spark.transformation.Combine;
import net.xingws.sample.spark.data.AverageCount;

/**
 * @author benxing
 *
 */
public class ActionExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ActionExample");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 3));
			//JavaRDD<Integer> rdd = ordd.cache();
			System.out.println(rdd.reduce((a, b) -> a + b));
			rdd = sc.parallelize(Arrays.asList(1, 2, 3, 3));
			System.out.println(rdd.fold(0, (a, b) -> a + b));
			rdd = sc.parallelize(Arrays.asList(1, 2, 3, 3));
			System.out.println(rdd.aggregate(new AverageCount(0,0), new AddAndCount(), new Combine()).avg());
			
		}
	}
}
