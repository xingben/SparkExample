/**
 * 
 */
package net.xingws.sample.spark.streaming.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.xingws.sample.spark.data.PoolConfiguration;

/**
 * @author benxing
 *
 */
public class HttpClientPool implements Serializable, Closeable {

	private static final long serialVersionUID = 36880722360378636L;
	private static HttpClientPool instance = null;
	private static final Logger log = LoggerFactory.getLogger(HttpClientPool.class);
	 private final CloseableHttpClient httpClient;
	 private final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
	 private ConnectionMonitor monitor;
	 private final long monitorCycle;
	 private final long idleConnectionCycleTimeout;
	 private volatile boolean shutdown = false;
	 private ExecutorService executor = Executors.newFixedThreadPool(1, new ThreadFactory() {

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r);
			t.setDaemon(true);
			return t;
		}
		 
	 }); 
	 
	 private HttpClientPool(PoolConfiguration config) {
		 this.connectionManager.setMaxTotal(config.getMaxConnection());		 
		 this.connectionManager.setDefaultMaxPerRoute(config.getMaxConntionPerRoute());
		 
		 this.httpClient = HttpClients.custom().setConnectionManager(connectionManager).build();
		 this.monitorCycle = config.getMonitorCycleTimeout();
		 this.idleConnectionCycleTimeout = config.getIdleConnectionLifeCycleTimeout();
		 this.initilize();
	 }
	 
	 public synchronized static HttpClientPool getInstance(PoolConfiguration config) {
		 if(instance == null) {
			 instance = new HttpClientPool(config);
		 }
		 
		 return instance;
	 }
	 
	 private void initilize() {
		 this.monitor = new HttpClientPool.ConnectionMonitor();
		 this.executor.submit(this.monitor);
	 }
	 
	 public CloseableHttpClient getClient() {
		 return this.httpClient;
	 }
	 
	 private class ConnectionMonitor implements Runnable {

		@Override
		public void run() {
			log.info("Connection Monitor started");
			try {
				while (!shutdown) {
					synchronized (this) {
						this.wait(monitorCycle);
						// close the expired connection
						connectionManager.closeExpiredConnections();

						// close the long idled connection
						connectionManager.closeIdleConnections(idleConnectionCycleTimeout, TimeUnit.MINUTES);
					}
				}
			} catch (InterruptedException e) {
				log.error(e.getMessage(), e);
			}
			
			log.info("Connection Monitor stoped");
		}
	 }

	@Override
	public void close() {
		synchronized (this) {
			this.shutdown = true;
			this.notifyAll();
		}
		
		try {
			this.httpClient.close();
		} catch (IOException e) {
			log.error("Exception happend during the closing of httpClient");
		}
		
		this.connectionManager.close();
	}
	
	@Override
	public void finalize(){
		this.close();
	}
}
