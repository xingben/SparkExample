/**
 * 
 */
package net.xingws.sample.spark.app;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import net.xingws.sample.sample.spark.transformation.StringToCharsFlatMap;

/**
 * @author benxing
 *
 */
public class FlatMapExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("FlatMapExample");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			JavaRDD<String> words = sc.parallelize(Arrays.asList("Hello", null, "world"));
			System.out.println(StringUtils.join(words.flatMap(new StringToCharsFlatMap()).collect(), ","));
		}
	}
}
