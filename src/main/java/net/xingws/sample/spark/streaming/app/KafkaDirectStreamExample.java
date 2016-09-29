/**
 * 
 */
package net.xingws.sample.spark.streaming.app;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

/**
 * @author benxing
 *
 */
public class KafkaDirectStreamExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String topics = "test";
		String brokers="benxing-linux3:9092,benxing-linux4:9092,benxing-linux2:9092";
	    SparkConf sparkConf = new SparkConf().setAppName("KafkaDirectStreamExample");
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

	    Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
	    Map<String, String> kafkaParams = new HashMap<>();
	    kafkaParams.put("metadata.broker.list", brokers);
	    
	    // Create direct kafka stream with brokers and topics
	    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
	        jssc,
	        String.class,
	        String.class,
	        StringDecoder.class,
	        StringDecoder.class,
	        kafkaParams,
	        topicsSet
	    );

	    messages.print();
	    
	    // Start the computation
	    jssc.start();
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
