/**
 * 
 */
package net.xingws.sample.spark.app;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author benxing
 *
 */
public class TransformExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TransformExample");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 3));
			JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5));
			
			System.out.println(StringUtils.join(rdd1.collect(), ","));
			System.out.println(StringUtils.join(rdd1.union(rdd2).collect(), ","));
			System.out.println(StringUtils.join(rdd1.intersection(rdd2).collect(), ","));
			System.out.println(StringUtils.join(rdd1.subtract(rdd2).collect(), ","));
		}
	}

}
