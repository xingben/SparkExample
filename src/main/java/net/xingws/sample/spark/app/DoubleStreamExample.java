/**
 * 
 */
package net.xingws.sample.spark.app;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import net.xingws.sample.sample.spark.transformation.IntergerToDoubleTransform;

/**
 * @author bxing
 *
 */
public class DoubleStreamExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DoubleStreamExample");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 3));
			JavaDoubleRDD drdd = rdd.mapToDouble(new IntergerToDoubleTransform());
			System.out.println(drdd.mean());
		}
	}
}
