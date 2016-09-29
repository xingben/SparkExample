/**
 * 
 */
package net.xingws.sample.spark.steaming.app;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * @author bxing
 *
 */
public class TransformationSample {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TransformationSample");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
		Map<String, Integer> topics = new HashMap<String, Integer>();
		topics.put("test", 1);
		JavaPairDStream<String, String> input = KafkaUtils.createStream(jssc, "localhost:2181", "TestGroup", topics);

		JavaPairDStream<String, String> input1 = input
				.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
						// TODO Auto-generated method stub
						return rdd.filter(new Function<Tuple2<String, String>, Boolean>() {

							private static final long serialVersionUID = 3471158106896307910L;

							@Override
							public Boolean call(Tuple2<String, String> tuple) throws Exception {
								// TODO Auto-generated method stub
								return tuple._1().startsWith("key1");
							}

						});
					}

				});

		input1.print();
		System.out.println(input1.repartition(10));

		List<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();

		input1.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {

			private static final long serialVersionUID = -8426501670149908775L;

			@Override
			public void call(JavaPairRDD<String, String> rdd) throws Exception {

				result.addAll(rdd.collect());
				System.out.println(result.size());
			}
		});

		// input.print();
		System.out.println(result.size());

		// start our streaming context and wait for it to "finish"
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			jssc.stop();
		}
	}
}
