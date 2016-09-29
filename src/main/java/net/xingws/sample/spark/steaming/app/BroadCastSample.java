/**
 * 
 */
package net.xingws.sample.spark.steaming.app;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import net.xingws.sample.spark.broadcast.TestGocConfigBroadCast;
import net.xingws.sample.spark.conf.TestGocConfig;
import scala.Tuple2;

/**
 * @author bxing
 *
 */
public class BroadCastSample {
	private static String checkpointDirectory = "/tmp/BroadCastSample";
	/**
	 * @param args
	 */
	public static JavaStreamingContext createContext() {
		SparkConf conf = new SparkConf().setAppName("BroadCastSample");
		
	    JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
	    jssc.checkpoint(checkpointDirectory);
	    Map<String, Integer> topics = new HashMap<String, Integer>();
	    topics.put("test", 3);
	    JavaPairDStream<String, String> input = KafkaUtils.createStream(jssc, "localhost:2181", "TestGroup", topics);
	    
	    input.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {

			private static final long serialVersionUID = -5314014090133914774L;

			@Override
			public void call(JavaPairRDD<String, String> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {

					private static final long serialVersionUID = 4641701868436056894L;
					TestGocConfig config = TestGocConfigBroadCast.getInstance(new JavaSparkContext(rdd.context())).getValue();
					@Override
					public void call(Iterator<Tuple2<String, String>> messages) throws Exception {
						
						System.out.println(config);
					}					
				});
			}
	    	
	    });
	    
	    input.print();
	    
	    return jssc;

	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
	    Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>() {

			private static final long serialVersionUID = 234L;

			@Override
	        public JavaStreamingContext call() {
	          return createContext();
	        }
	      };

	      JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);
	      ssc.start();
	      ssc.awaitTermination();
	}

}
