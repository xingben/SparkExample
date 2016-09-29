/**
 * 
 */
package net.xingws.sample.spark.streaming.app;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import net.xingws.sample.spark.data.PoolConfiguration;
import scala.Tuple2;

/**
 * @author benxing
 *
 */
public class BroadCastSample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
        PoolConfiguration poolConfig = new PoolConfiguration();
        poolConfig.setMaxConnection(20);
        poolConfig.setMaxConntionPerRoute(10);
        poolConfig.setMonitorCycleTimeout(10000);
        poolConfig.setIdleConnectionLifeCycleTimeout(1);
        
		SparkConf conf = new SparkConf().setAppName("BroadCastSample");
		
	    JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
	    Map<String, Integer> topics = new HashMap<String, Integer>();
	    topics.put("test", 3);
	    JavaPairDStream<String, String> input = KafkaUtils.createStream(jssc, "benxing-linux2:2181", "TestGroup", topics);
	    
	    Broadcast<PoolConfiguration> bc_config = jssc.sparkContext().broadcast(poolConfig);
	    
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
