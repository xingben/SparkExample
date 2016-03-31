/**
 * 
 */
package net.xingws.sample.spark.app;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author benxing
 *
 */
public class TwoPairTransformExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TwoPairTransformExample");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2<Integer, Integer>(4,2), 
					new Tuple2<Integer, Integer>(3, 4), 
					new Tuple2<Integer, Integer>(3, 6)));
			
			JavaPairRDD<Integer, Integer> rdd2 = sc.parallelizePairs(Arrays.asList(new Tuple2<Integer, Integer>(3,9)));
			
			System.out.println(rdd1.subtractByKey(rdd2).collect());
			System.out.println(rdd1.join(rdd2).collect());
			System.out.println(rdd1.rightOuterJoin(rdd2).collect());
			System.out.println(rdd1.leftOuterJoin(rdd2).collect());
			System.out.println(rdd1.cogroup(rdd2).collect());
		}
	}

}
