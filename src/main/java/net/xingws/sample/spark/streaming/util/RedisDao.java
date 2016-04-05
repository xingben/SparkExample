/**
 * 
 */
package net.xingws.sample.spark.streaming.util;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.xingws.sample.spark.data.DataRecord;
import redis.clients.jedis.Jedis;

/**
 * @author bxing
 *
 */
public class RedisDao implements Dao<DataRecord> {

	private ObjectMapper mapper = new ObjectMapper();
	private RedisPool pool;
	private String key;
	
	public RedisDao(String key, String hostname, int port, int maxConnection) {
		this.key = key;
		this.pool = new RedisPool(hostname, port, maxConnection);
	}

	@Override
	public void enqueue(DataRecord record) {
		Jedis jedis = this.pool.getRedisPool().getResource();
		
		try {
			try {
				jedis.lpush(key, mapper.writeValueAsString(record));
			} catch (JsonProcessingException e) {
				//ignore the error for now.
			}
		} finally {
			jedis.close();
		}
	}

	@Override
	public DataRecord dequeue() {
		Jedis jedis = this.pool.getRedisPool().getResource();
		DataRecord record = null;
		
		try {
				record = mapper.readValue(jedis.brpop(0, key).get(1), DataRecord.class);

		} catch (JsonParseException e) {
		} catch (JsonMappingException e) {
		} catch (IOException e) {
		} finally {
			jedis.close();
		}
		
		return record;
	}
}
