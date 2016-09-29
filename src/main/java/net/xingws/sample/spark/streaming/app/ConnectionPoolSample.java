/**
 * 
 */
package net.xingws.sample.spark.streaming.app;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import net.xingws.sample.spark.data.PoolConfiguration;
import net.xingws.sample.spark.streaming.util.HttpClientPool;
import net.xingws.sample.spark.streaming.util.RequestClient;
import scala.Tuple2;

class Configuration {

	private static volatile Broadcast<PoolConfiguration> instance = null;

	public static Broadcast<PoolConfiguration> getInstance(JavaSparkContext jsc) {
		if (instance == null) {
			synchronized (Configuration.class) {
				if (instance == null) {
					PoolConfiguration poolConfig = new PoolConfiguration();
					poolConfig.setMaxConnection(20);
					poolConfig.setMaxConntionPerRoute(10);
					poolConfig.setMonitorCycleTimeout(10000);
					poolConfig.setIdleConnectionLifeCycleTimeout(1);
					instance = jsc.broadcast(poolConfig);
				}
			}
		}
		return instance;
	}
}

/**
 * @author benxing
 *
 */
public class ConnectionPoolSample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ConnectionPoolSample");
		
	    JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
	    
	    Runtime.getRuntime().addShutdownHook(new Thread() {
	    	public void run() {
	    		jssc.stop();
   		
	    	}
	    });
	    
/*        PoolConfiguration poolConfig = new PoolConfiguration();
        poolConfig.setMaxConnection(20);
        poolConfig.setMaxConntionPerRoute(10);
        poolConfig.setMonitorCycleTimeout(10000);
        poolConfig.setIdleConnectionLifeCycleTimeout(1);*/
	    
	    Map<String, Integer> topics = new HashMap<String, Integer>();
	    topics.put("test", 3);
	    JavaPairDStream<String, String> input = KafkaUtils.createStream(jssc, "benxing-linux2:2181", "TestGroup", topics);
	    
	    input.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {

			private static final long serialVersionUID = -5314014090133914774L;

			@Override
			public void call(JavaPairRDD<String, String> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {

					private static final long serialVersionUID = 4641701868436056894L;
					PoolConfiguration poolConfig = Configuration.getInstance(new JavaSparkContext(rdd.context())).getValue();
					@Override
					public void call(Iterator<Tuple2<String, String>> messages) throws Exception {
						RequestClient rc = new RequestClient(HttpClientPool.getInstance(poolConfig).getClient());
						while(messages.hasNext()) {
							System.out.print(rc.get("http://www.google.com"));
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
	  //  jssc.stop();
	}
}
