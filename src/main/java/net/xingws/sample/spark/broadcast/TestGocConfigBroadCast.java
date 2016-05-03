/**
 * 
 */
package net.xingws.sample.spark.broadcast;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.google.inject.Guice;
import com.google.inject.Injector;

import net.xingws.sample.spark.conf.TestGocConfig;
import net.xingws.sample.spark.guice.TestGocConfigModule;

/**
 * @author bxing
 *
 */
public class TestGocConfigBroadCast {
	private static volatile Broadcast<TestGocConfig> instance = null;
	
	public static Broadcast<TestGocConfig> getInstance(JavaSparkContext jsc) {
		if(instance == null) {
			synchronized(TestGocConfigBroadCast.class) {
				if(instance == null) {
					Injector injector = Guice.createInjector(new TestGocConfigModule());
					TestGocConfig testGocConfig = injector.getInstance(TestGocConfig.class);
					instance = jsc.broadcast(testGocConfig);
				}
			}
		}
		
		return instance;
	}
}
