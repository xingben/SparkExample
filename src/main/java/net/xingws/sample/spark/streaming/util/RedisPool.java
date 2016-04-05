/**
 * 
 */
package net.xingws.sample.spark.streaming.util;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author benxing
 *
 */
public class RedisPool {
	private JedisPool pool;

	public RedisPool(String host, 
			int port, 
			int maxConnection) {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setBlockWhenExhausted(false);
		config.setMaxTotal(maxConnection);
		this.pool = new JedisPool(config, host, port);
	}
	
	public JedisPool getRedisPool() {
		return this.pool;
	}
}
