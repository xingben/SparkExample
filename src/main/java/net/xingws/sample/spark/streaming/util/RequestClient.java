/**
 * 
 */
package net.xingws.sample.spark.streaming.util;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author bxing
 *
 */
public class RequestClient implements Serializable {

	private static final long serialVersionUID = -4584257563597988526L;
	private static final Logger log = LoggerFactory.getLogger(RequestClient.class);
	private static final Charset UTF_8 = Charset.forName("UTF-8");
	private int connectionTimeout = 3000;
	private int socketTimeout = 20000;
	private CloseableHttpClient httpClient;
	
	public RequestClient(CloseableHttpClient httpClient) {
		this.httpClient = httpClient;
	}
	
	public final String get(String requestUrl)
			throws Exception{
		String response = null;
		CloseableHttpResponse httpResponse = null;
		
		RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(this.connectionTimeout)
				.setConnectTimeout(this.connectionTimeout).setSocketTimeout(this.socketTimeout).build();

		HttpGet get = new HttpGet(requestUrl);
		get.setConfig(requestConfig);
		
		try {
			httpResponse = httpClient.execute(get); 
			
			if(httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				EntityUtils.consume(httpResponse.getEntity());
				log.error("Http Return Error Code {}",  httpResponse.getStatusLine().getStatusCode());
				throw new Exception("Failed to get request : " + httpResponse.getStatusLine().getStatusCode());
			}
			
			HttpEntity entity = httpResponse.getEntity();
			if (entity != null) {
				response = EntityUtils.toString(entity, UTF_8);
			}

		} catch (UnsupportedEncodingException e) {
			log.error("Request failed with exception" , e);
		} catch (ClientProtocolException e) {
			log.error("Request failed with exception" , e);
		} catch (IOException e) {
			log.error("Request failed with exception" , e);
		} finally {
			if(httpResponse != null) {
				try {
					httpResponse.close();
				} catch (IOException e) {
					log.error("Can not close the response" , e);
				}
			}
		}
		
		return response;
	}
}
