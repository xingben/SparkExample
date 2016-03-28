/**
 * 
 */
package net.xingws.sample.sample.spark.transformation;

import org.apache.spark.api.java.function.DoubleFunction;

/**
 * @author bxing
 *
 */
public class IntergerToDoubleTransform implements DoubleFunction<Integer> {

	private static final long serialVersionUID = -7206661519346392034L;

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.DoubleFunction#call(java.lang.Object)
	 */
	@Override
	public double call(Integer input) throws Exception {
		
		return (double) input;
	}

}
