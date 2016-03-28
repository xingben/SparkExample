/**
 * 
 */
package net.xingws.sample.spark.app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import net.xingws.sample.sample.spark.transformation.ContainsFilter;
import net.xingws.sample.sample.spark.transformation.WordCountMap;

/**
 * @author benxing
 *
 */
public class MapExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String inputFile = args[0];
		String outputFile = args[1];
		
		SparkConf conf = new SparkConf().setAppName("MapExample");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			JavaRDD<String> input = sc.textFile(inputFile);
			input.filter(new ContainsFilter("Python")).map(new WordCountMap()).saveAsTextFile(outputFile);
		}
	}

}
