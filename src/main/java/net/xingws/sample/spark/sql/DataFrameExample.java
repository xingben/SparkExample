/**
 * 
 */
package net.xingws.sample.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.api.java.function.FilterFunction;

/**
 * @author benxing
 *
 */
public class DataFrameExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName(DataFrameExample.class.getCanonicalName())
				.config("key1", "value1")
				.getOrCreate();
		
		System.out.println(spark.conf().get("key1"));
		
		Dataset<Row> df = spark.read().json(args[0]);
		
		df.printSchema();
		df.select("name").show();
		df.select("name", "age").show();
		df.select(col("name"), col("age").plus(2)).show();
		df.filter(col("age").gt(21)).show();
		df.filter(new TestFilter()).show();;
		df.filter(row -> {return row.isNullAt(0) ? false : row.getLong(0) > 22;});
		df.groupBy(col("age")).count();
		df.limit(1).show();
	
		df.createOrReplaceTempView("view");
		spark.sql("select * from view where age < 50 and name = 'Andy'").show();
		
		df.write().json("/home/benxing/Applications/spark-2.0.0-bin-hadoop2.6/output/jsontest");
	}

}

class TestFilter implements FilterFunction<Row> {

	private static final long serialVersionUID = -527760587804228742L;

	@Override
	public boolean call(Row row) throws Exception {
		
		return row.isNullAt(0) ? false : row.getLong(0) > 22;
	}
	
}
