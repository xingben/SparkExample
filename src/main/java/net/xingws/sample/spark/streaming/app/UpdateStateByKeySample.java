/**
 * 
 */
package net.xingws.sample.spark.streaming.app;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

/**
 * @author benxing
 *
 */
public class UpdateStateByKeySample {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static String checkpointDirectory = "/tmp/spark13";
	//private static 
	
	private static JavaStreamingContext createContext() {
		
		
		SparkConf conf = new SparkConf().setAppName("UpdateStateByKeySample");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
		JavaDStream<String> lines = ssc.socketTextStream("benxing-linux1", 7777);

		ssc.checkpoint(checkpointDirectory);

		JavaPairDStream<String, Integer> wordsDstream = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 9138776695249890108L;

			@Override
			public Iterator<String> call(String x) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(SPACE.split(x)).iterator();
			}
		}).mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 8066904520315124506L;

			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {

				return new Tuple2<>(arg0, 1);
			}
		});
		
		JavaPairDStream<String, Integer> responseCodeCountDStream = wordsDstream.updateStateByKey(new UpdateRunningSum());
		
		responseCodeCountDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {

			private static final long serialVersionUID = -2275569179152601745L;

			@Override
			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {

					private static final long serialVersionUID = -1910295233232756383L;

					@Override
					public void call(Iterator<Tuple2<String, Integer>> it) throws Exception {
						while(it.hasNext()){
							Tuple2<String, Integer> t = it.next(); 
						}
					}
				});	
			}
		});
		
		responseCodeCountDStream.print();
			
		return ssc;
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
	    Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>() {

			private static final long serialVersionUID = 1L;

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

class UpdateRunningSum implements Function2<List<Integer>, Optional<Integer>, Optional<Integer>> {

	private static final long serialVersionUID = -4156315138924642092L;
	
	public UpdateRunningSum() {
	}

	public Optional<Integer> call(List<Integer> nums, Optional<Integer> current) {
		Integer sum = current.or(0);
		Integer total = sum + nums.size();
		
		//if(total > 5) {

		//	return Optional.absent();
		//}
		System.out.println(total);
		
		return Optional.of(total);  
	}
}

