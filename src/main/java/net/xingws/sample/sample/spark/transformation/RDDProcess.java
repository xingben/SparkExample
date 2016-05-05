/**
 * 
 */
package net.xingws.sample.sample.spark.transformation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import net.xingws.sample.spark.data.PoolConfiguration;

/**
 * @author benxing
 *
 */
public class RDDProcess implements VoidFunction<JavaPairRDD<String, String>> {

	private static final long serialVersionUID = -8147742327206118644L;
	private Broadcast<PoolConfiguration> bc_config;

	public RDDProcess(Broadcast<PoolConfiguration> bc_config) {
		this.bc_config = bc_config;
	}
	
	@Override
	public void call(JavaPairRDD<String, String> t) throws Exception {
		PoolConfiguration config = bc_config.getValue();
		System.out.println(config.getIdleConnectionLifeCycleTimeout());
		
		//t.foreachPartition(f);
		
	}

}
