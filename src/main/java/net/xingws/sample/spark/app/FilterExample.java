/**
 * 
 */
package net.xingws.sample.spark.app;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.xingws.sample.sample.spark.transformation.ContainsFilter;

/**
 * @author benxing
 *
 */
public class FilterExample {
	private static Logger log = LoggerFactory.getLogger(FilterExample.class);
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String inputFile = args[0];
		String outputFile = args[1];
		
		log.info("Process is started");
		SparkConf conf = new SparkConf().setAppName("FilterExample");
		JavaSparkContext sc = new JavaSparkContext(conf);// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);// Split up into words.

		JavaRDD<String> pythonRDD = input.filter(new ContainsFilter("Python"));
		pythonRDD.saveAsTextFile(outputFile);	
		log.info("Process is end");
		sc.close();
	}

}
