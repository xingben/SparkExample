package net.xingws.sample.spark.sql.streaming;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import scala.Tuple2;

public class WordCountExample {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
			    .builder()
			    .appName(WordCountExample.class.getCanonicalName())
			    .getOrCreate();
		
		Dataset<Row> lines = spark
		.readStream().format("socket")
		.option("host", "localhost")
		.option("port", 9999)
		.option("includeTimestamp", true)
		.load();
		
		lines.printSchema();
		
		Dataset<String> words = lines
		.as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
		.flatMap(
		        new FlatMapFunction<Tuple2<String, Timestamp>, String>() {

					private static final long serialVersionUID = 5838131373841260802L;

					@Override
		            public Iterator<String> call(Tuple2<String, Timestamp> x) {
		              return Arrays.asList(x._1.split(" ")).iterator();
		            }
		          }, Encoders.STRING());
		
	    StreamingQuery query  = words.toDF("value").groupBy("value").count()
	    		.writeStream()
	    		.outputMode("complete")
	    		.format("console")
	    		.start();
	    
	    query.awaitTermination();
	}

}
