package net.xingws.sample.sample.spark.transformation;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import net.xingws.sample.spark.data.PoolConfiguration;
import scala.Tuple2;

public class ParitionProcss implements VoidFunction<Tuple2<String, String>> {

	private static final long serialVersionUID = 2344240677299131461L;
	private Broadcast<PoolConfiguration> bc_config;
	
	public ParitionProcss(Broadcast<PoolConfiguration> bc_config) {
		this.bc_config = bc_config;
	}
	
	@Override
	public void call(Tuple2<String, String> t) throws Exception {
		PoolConfiguration config = this.bc_config.getValue();
		System.out.println(config.getMaxConntionPerRoute());
		
	}

}
