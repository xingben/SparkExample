/**
 * 
 */
package net.xingws.sample.sample.spark.transformation;

import org.apache.spark.api.java.function.Function;

import net.xingws.sample.spark.data.AverageCount;

/**
 * @author bxing
 *
 */
public class CreateAverageCount implements Function<Integer, AverageCount> {

	private static final long serialVersionUID = 8044281899105594973L;

	@Override
	public AverageCount call(Integer arg0) throws Exception {
		// TODO Auto-generated method stub
		return new AverageCount(arg0, 1);
	}
}
