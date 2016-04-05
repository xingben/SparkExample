/**
 * 
 */
package net.xingws.sample.spark.test.app;

import java.util.Date;

import net.xingws.sample.spark.data.DataRecord;
import net.xingws.sample.spark.streaming.util.RedisDao;

/**
 * @author bxing
 *
 */
public class CustomerReceiverSampleTest {

	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		RedisDao dao = new RedisDao("test", "localhost", 6379, 8);
		
		for (int i=0;; i++) {
			DataRecord record = new DataRecord();
			record.setKey("key"+i);
			record.setScore((new Date()).getTime());
			dao.enqueue(record);
			Thread.sleep(1000);
		}
	}

}
