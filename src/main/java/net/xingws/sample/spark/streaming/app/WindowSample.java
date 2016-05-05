/**
 * 
 */
package net.xingws.sample.spark.streaming.app;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

/**
 * @author benxing
 *
 */
public class WindowSample {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static String checkpointDirectory = "/tmp/spark-WindowSample";

	private static JavaStreamingContext createContext() {
		Configuration twitterConf = ConfigurationContext
				.getInstance("/home/benxing/Applications/spark-1.6.1-bin-hadoop2.6");
		Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

		String[] filters = { "#NBA" };

		SparkConf conf = new SparkConf().setAppName("WindowSample");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(15));
		ssc.checkpoint(checkpointDirectory);

		JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(ssc, twitterAuth, filters);
		
		//twitterStream.print();

		JavaPairDStream<String, Integer> tuples  = twitterStream.map(status -> status.getText())
				.flatMap(text -> Arrays.asList(SPACE.split(text)))
				.filter(word -> word.startsWith("#"))
				.mapToPair(hashTag-> new Tuple2<>(hashTag,1));
		
		JavaPairDStream<String, Integer> counts =  tuples.reduceByKeyAndWindow(
			      new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					public Integer call(Integer i1, Integer i2) { return i1 + i2; }
			      },
			      new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					public Integer call(Integer i1, Integer i2) { return i1 - i2; }
			      },
			      Durations.seconds(60),
			      Durations.seconds(15)
			    );
//			    ).transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>(){
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> v1) throws Exception {
//						// TODO Auto-generated method stub
//						return v1.sort;
//					}
//			    	
//			    });

		counts.print();
		return ssc;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
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
		ssc.close();
	}

}
