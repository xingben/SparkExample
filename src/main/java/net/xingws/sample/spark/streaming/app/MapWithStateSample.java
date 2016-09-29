/**
 * 
 */
package net.xingws.sample.spark.streaming.app;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;

/**
 * @author benxing
 *
 */
public class MapWithStateSample implements Serializable {

	private static final long serialVersionUID = -4435854284388831303L;
	private static final Pattern SPACE = Pattern.compile(" ");
	private static String checkpointDirectory = "/tmp/MapWithStateSample";
	
	
	private static JavaStreamingContext createContext() {
		
		
		SparkConf conf = new SparkConf().setAppName("MapWithStateSample");
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
		
		//wordsDstream.mapWithState(StateSpec<String, Integer, StateType, MappedType>.function(arg0)<String, Integer, MapState, Tuple2<String, Integer>>)
		
		Function3<String, Optional<Integer>, State<MapState>, Tuple2<String, Integer>> mappingFunction = 
				new Function3<String, Optional<Integer>, State<MapState>, Tuple2<String, Integer>>() {

					private static final long serialVersionUID = 367361675300404498L;

					@Override
					public Tuple2<String, Integer> call(String key, Optional<Integer> value, State<MapState> state)
							throws Exception {
						int v = value.orElse(0) + (state.exists() ? state.get().getValue() : 0);
						long time = (new Date().getTime());
						MapWithStateSample sample = new MapWithStateSample();
						if(state.isTimingOut()) {
							return null;
						}
						state.update(sample.new MapState(key, v, time));			
						return new Tuple2<String, Integer>(key, v);
					}
			
		};
		
		JavaMapWithStateDStream<String, Integer, MapState, Tuple2<String, Integer>> stateDstream =
		        wordsDstream.mapWithState(StateSpec.function(mappingFunction).timeout(Durations.seconds(20)));
		
		//stateDstream.print();
		
		JavaDStream<Tuple2<String, Integer>> s = stateDstream.filter(new Function<Tuple2<String, Integer>, Boolean>() {

			private static final long serialVersionUID = 3475977421126839851L;

			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				// TODO Auto-generated method stub
				return v1 != null;
			}
			
		});
		
		s.print();
		
		//JavaPairDStream<String, MapState> s = stateDstream.stateSnapshots();
		//s.print();
		
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
	      try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	class MapState implements Serializable{

		private static final long serialVersionUID = 8351950820011095786L;
		private String key;
		private Integer value;
		private long timestamp;
		
		public MapState(String key, Integer value, long timestamp) {
			this.key = key;
			this.value = value;
			this.timestamp = timestamp;
		}

		@Override
		public String toString() {
			return String.format("%s-%d-%d", key, value, timestamp);
		}
		/**
		 * @return the key
		 */
		public String getKey() {
			return key;
		}

		/**
		 * @param key the key to set
		 */
		public void setKey(String key) {
			this.key = key;
		}

		/**
		 * @return the value
		 */
		public Integer getValue() {
			return value;
		}

		/**
		 * @param value the value to set
		 */
		public void setValue(Integer value) {
			this.value = value;
		}

		/**
		 * @return the timestamp
		 */
		public long getTimestamp() {
			return timestamp;
		}

		/**
		 * @param timestamp the timestamp to set
		 */
		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}
	}
}
