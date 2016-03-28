/**
 * 
 */
package net.xingws.sample.spark.data;

import java.io.Serializable;

/**
 * @author benxing
 *
 */
public class AverageCount implements Serializable {

	private static final long serialVersionUID = -4193552279498563701L;

	public AverageCount(int total, int num) {
		this.total = total;
		this.num = num;
	}

	public int total;
	public int num;

	public double avg() {
		return total / (double) num;
	}
}
