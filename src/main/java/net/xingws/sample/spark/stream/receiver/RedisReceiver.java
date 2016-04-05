/**
 * 
 */
package net.xingws.sample.spark.stream.receiver;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import net.xingws.sample.spark.data.DataRecord;
import net.xingws.sample.spark.streaming.util.Dao;
import net.xingws.sample.spark.streaming.util.RedisDao;

/**
 * @author bxing
 *
 */
public class RedisReceiver extends Receiver<DataRecord> {
	private static final long serialVersionUID = -2015513131668774806L;

	private String hostname;
	private int port;
	private int maxConnection;
	private String key;
	private Dao<DataRecord> dao;
	
	public RedisReceiver(String key, String hostname, int port, int maxConnection) {
		super(StorageLevel.MEMORY_AND_DISK_SER_2());
		this.hostname = hostname;
		this.port = port;
		this.maxConnection = maxConnection;
		this.key = key;
	}

	@Override
	public void onStart() {
		this.dao = new RedisDao(this.key, this.hostname, this.port, this.maxConnection);
	    new Thread()  {
	        @Override public void run() {
	          receive();
	        }
	      }.start();
	}

	@Override
	public void onStop() {

	}
	
	private void receive() {
		try {
			while(true) {
				DataRecord record = this.dao.dequeue();
				this.store(record);
			}
		} catch(Exception ex) {
			System.out.println(ex);
		}
		
		this.restart("restart the receiver");
	}
}
