/**
 * 
 */
package net.xingws.sample.spark.data;

import java.io.Serializable;

/**
 * @author bxing
 *
 */
public class DataRecord implements Serializable {

	private static final long serialVersionUID = 6768356931307476172L;
	
	private String key;
	private long score;
	
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public long getScore() {
		return score;
	}

	public void setScore(long score) {
		this.score = score;
	}

}
