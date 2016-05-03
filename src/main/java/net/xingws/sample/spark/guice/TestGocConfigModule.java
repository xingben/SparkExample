/**
 * 
 */
package net.xingws.sample.spark.guice;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import net.xingws.sample.spark.conf.TestGocConfig;

/**
 * @author bxing
 *
 */
public class TestGocConfigModule extends AbstractModule {

	/* (non-Javadoc)
	 * @see com.google.inject.AbstractModule#configure()
	 */
	@Override
	protected void configure() {
		this.bind(String.class).annotatedWith(Names.named("username")).toInstance("cba");
		this.bind(String.class).annotatedWith(Names.named("email")).toInstance("cba@gmail.com");
		this.bind(String.class).annotatedWith(Names.named("pin")).toInstance("4321");
		this.bind(TestGocConfig.class).in(Singleton.class);
	}

}
