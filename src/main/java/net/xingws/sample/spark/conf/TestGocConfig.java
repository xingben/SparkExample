/**
 * 
 */
package net.xingws.sample.spark.conf;

import java.io.Serializable;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * @author bxing
 *
 */
public class TestGocConfig implements Serializable {

	private static final long serialVersionUID = 5828049299918987691L;
	private String username;
	private String email;
	private String pin;
	
	@Inject
	public TestGocConfig(@Named("username") String username, @Named("email")String email, @Named("pin")String pin) {
		this.username = username;
		this.email = email;
		this.pin = pin;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getPin() {
		return pin;
	}

	public void setPin(String pin) {
		this.pin = pin;
	}
	
	@Override
	public String toString() {
		return String.format("username=%s; email=%s; pin=%s", this.username, this.email, this.pin);
	}
}
