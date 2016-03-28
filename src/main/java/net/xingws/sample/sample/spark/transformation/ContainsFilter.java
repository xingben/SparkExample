/**
 * 
 */
package net.xingws.sample.sample.spark.transformation;

import org.apache.spark.api.java.function.Function;

/**
 * @author benxing
 *
 */
public class ContainsFilter implements Function<String, Boolean> {

	private static final long serialVersionUID = 1146761738809818538L;
	private String query;
	
	public ContainsFilter(String query) {
		this.query = query;
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
	 */
	@Override
	public Boolean call(String input) throws Exception {
		// TODO Auto-generated method stub
		return input == null ? false : input.contains(query);
	}

}
