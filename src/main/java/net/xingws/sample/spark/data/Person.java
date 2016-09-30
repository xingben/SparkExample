/**
 * 
 */
package net.xingws.sample.spark.data;

import java.io.Serializable;

/**
 * @author benxing
 *
 */
public class Person implements Serializable {
	private static final long serialVersionUID = 5392818618483491575L;
	private String name;
	private long age;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getAge() {
		return age;
	}

	public void setAge(long age) {
		this.age = age;
	}
}