/**
 * 
 */
package net.xingws.sample.sample.spark.transformation;

import org.apache.spark.api.java.function.Function;


/**
 * @author benxing
 *
 */
public class WordCountMap implements Function<String, Integer> {

	private static final long serialVersionUID = 2075074033724390828L;

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
	 */
	@Override
	public Integer call(String input) throws Exception {
		// TODO Auto-generated method stub
		if(input == null) return 0;
		
		return input.split(" ").length;
	}

}
