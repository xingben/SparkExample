/**
 * 
 */
package net.xingws.sample.sample.spark.transformation;

import java.util.ArrayList;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.google.common.primitives.Chars;

/**
 * @author benxing
 *
 */
public class StringToCharsFlatMap implements FlatMapFunction<String, Character> {

	private static final long serialVersionUID = 4252582441706458917L;

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterable<Character> call(String input) throws Exception {
		if(input == null) return new ArrayList<Character>();
		
		return Chars.asList(input.toCharArray());
	}

}
