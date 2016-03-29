/**
 * 
 */
package net.xingws.sample.spark.app;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import net.xingws.sample.sample.spark.transformation.AddAndCount;
import net.xingws.sample.sample.spark.transformation.Combine;
import net.xingws.sample.sample.spark.transformation.CreateAverageCount;
import scala.Tuple2;

/**
 * @author bxing
 *
 */
public class SinglePairTransformExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ActionExample");
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			JavaPairRDD<Integer, Integer> pairrdd = sc.parallelizePairs(Arrays.asList(new Tuple2<Integer, Integer>(4,2), 
					new Tuple2<Integer, Integer>(3, 4), 
					new Tuple2<Integer, Integer>(3, 6)));
			
			pairrdd.persist(StorageLevel.MEMORY_ONLY());
			System.out.println(pairrdd.reduceByKey((a, b) -> Math.max(a, b)).collect());
			System.out.println(pairrdd.groupByKey().collect());
			System.out.println(pairrdd.keys().collect());
			System.out.println(pairrdd.values().collect());
			System.out.println(pairrdd.mapValues((a)-> (double) a).collect());
			System.out.println(pairrdd.sortByKey().collect());
			System.out.println(pairrdd.flatMapValues((a) -> Arrays.asList(a, a+1)));
			System.out.println(pairrdd.combineByKey(new CreateAverageCount(), new AddAndCount(), new Combine()));
			
			pairrdd.unpersist();
		}

	}

}
