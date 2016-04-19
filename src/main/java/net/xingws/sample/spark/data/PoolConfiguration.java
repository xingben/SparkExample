/**
 * 
 */
package net.xingws.sample.spark.data;

import java.io.Serializable;

/**
 * @author benxing
 *
 */
public class PoolConfiguration implements Serializable {

	private static final long serialVersionUID = 3644347295393455179L;
	private int maxConnection;
	private int maxConntionPerRoute;
	private long monitorCycleTimeout;
	private long idleConnectionLifeCycleTimeout;
	
	/**
	 * @return the monitorCycleTimeout
	 */
	public long getMonitorCycleTimeout() {
		return monitorCycleTimeout;
	}
	
	/**
	 * @param monitorCycleTimeout the monitorCycleTimeout to set
	 */
	public void setMonitorCycleTimeout(long monitorCycleTimeout) {
		this.monitorCycleTimeout = monitorCycleTimeout;
	}
	
	/**
	 * @return the idleConnectionLifeCycleTimeout
	 */
	public long getIdleConnectionLifeCycleTimeout() {
		return idleConnectionLifeCycleTimeout;
	}
	
	/**
	 * @param idleConnectionLifeCycleTimeout the idleConnectionLifeCycleTimeout to set
	 */
	public void setIdleConnectionLifeCycleTimeout(long idleConnectionLifeCycleTimeout) {
		this.idleConnectionLifeCycleTimeout = idleConnectionLifeCycleTimeout;
	}
	
	/**
	 * @return the maxConnection
	 */
	public int getMaxConnection() {
		return maxConnection;
	}
	
	/**
	 * @param maxConnection the maxConnection to set
	 */
	public void setMaxConnection(int maxConnection) {
		this.maxConnection = maxConnection;
	}
	
	/**
	 * @return the maxConntionPerRoute
	 */
	public int getMaxConntionPerRoute() {
		return maxConntionPerRoute;
	}
	
	/**
	 * @param maxConntionPerRoute the maxConntionPerRoute to set
	 */
	public void setMaxConntionPerRoute(int maxConntionPerRoute) {
		this.maxConntionPerRoute = maxConntionPerRoute;
	}
}
