package net.xingws.sample.sample.spark.transformation;

import org.apache.spark.api.java.function.Function2;

import net.xingws.sample.spark.data.AverageCount;

public class AddAndCount implements Function2<AverageCount, Integer, AverageCount> {

	private static final long serialVersionUID = -35863327781378825L;

	@Override
	public AverageCount call(AverageCount a, Integer b) throws Exception {
		a.total += b;
		a.num += 1;
		return a;
	}
}
