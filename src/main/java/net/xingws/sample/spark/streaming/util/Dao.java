/**
 * 
 */
package net.xingws.sample.spark.streaming.util;

/**
 * @author bxing
 *
 */
public interface Dao<T> {
	void enqueue(T record);
	T dequeue();
}
