/**
 * 
 */
package net.xingws.sample.spark.steaming.app;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import net.xingws.sample.spark.data.DataRecord;
import net.xingws.sample.spark.stream.receiver.RedisReceiver;

/**
 * @author bxing
 *
 */
public class CustomerReceiverSample {


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CustomerReceiverSample");
		
	    JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
	    JavaDStream<DataRecord> customReceiverStream = jssc.receiverStream(new RedisReceiver("test", "localhost", 6379, 8));
	    customReceiverStream.foreachRDD(new VoidFunction<JavaRDD<DataRecord>>() {

			private static final long serialVersionUID = 1L;


			@Override
			public void call(JavaRDD<DataRecord> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<DataRecord>>() {

					private static final long serialVersionUID = 4641701868436056894L;

					@Override
					public void call(Iterator<DataRecord> messages) throws Exception {
						while(messages.hasNext()) {
							DataRecord message = messages.next();
							System.out.println(message);
						}
							
					}
					
				});
				
			}
	    });
	    
	    // start our streaming context and wait for it to "finish"
	    jssc.start();
	    jssc.awaitTermination();
	    jssc.stop();
	    jssc.close();
	}

}
