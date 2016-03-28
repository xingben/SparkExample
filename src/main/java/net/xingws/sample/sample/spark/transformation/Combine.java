/**
 * 
 */
package net.xingws.sample.sample.spark.transformation;

import org.apache.spark.api.java.function.Function2;

import net.xingws.sample.spark.data.AverageCount;

/**
 * @author benxing
 *
 */
public class Combine implements Function2<AverageCount, AverageCount, AverageCount> {

	private static final long serialVersionUID = 608431681431258666L;

	@Override
	public AverageCount call(AverageCount a, AverageCount b) throws Exception {
		a.total += b.total;
		a.num += b.num;
		
		return a;
	}

}
