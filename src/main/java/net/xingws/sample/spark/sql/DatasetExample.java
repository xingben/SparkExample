/**
 * 
 */
package net.xingws.sample.spark.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.xingws.sample.spark.data.Person;

/**
 * @author benxing
 *
 */
public class DatasetExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName(DatasetExample.class.getCanonicalName())
				.getOrCreate();

		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);
		
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		
		Dataset<Person> persons = spark.read().json(args[0]).as(personEncoder);
		
		persons.show();
		persons.printSchema();
		
		Dataset<Long> ages = persons.toDF().map(new MapFunction<Row, Long>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Row person) throws Exception {
				// TODO Auto-generated method stub
				return person.isNullAt(0) ? 0L : person.getLong(0);
			}
			
		}, Encoders.LONG());
		
		ages.show();
	}

}
