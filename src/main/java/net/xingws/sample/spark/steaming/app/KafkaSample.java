/**
 * 
 */
package net.xingws.sample.spark.steaming.app;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * @author benxing
 *
 */
public class KafkaSample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("KafkaSample");
		
	    JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
	    Map<String, Integer> topics = new HashMap<String, Integer>();
	    topics.put("test", 3);
	    JavaPairDStream<String, String> input = KafkaUtils.createStream(jssc, "benxing-linux2:2181", "TestGroup", topics);
	    
	    input.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {

			private static final long serialVersionUID = -5314014090133914774L;

			@Override
			public void call(JavaPairRDD<String, String> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {

					private static final long serialVersionUID = 4641701868436056894L;

					@Override
					public void call(Iterator<Tuple2<String, String>> messages) throws Exception {
						while(messages.hasNext()) {
							Tuple2<String, String> message = messages.next();
							System.out.println(message._2);
						}
							
					}
					
				});
			}
	    	
	    });
	    
	    //input.print();
	    
	    // start our streaming context and wait for it to "finish"
	    jssc.start();
	    jssc.awaitTermination();
	    jssc.stop();
	}

}
