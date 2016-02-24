/**
 * 
 */
package unipg.gila.common.datastructures;

import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;

/**
 * A set of LongWritable(s). It extends the abstract class SetWritable<P extends Writable>.
 * 
 * @author Alessio Arleo
 *
 */
public class LongWritableSet extends SetWritable<LongWritable> {
	
	/**
	 * Parameter-less constructor.
	 * 
	 */
	public LongWritableSet() {
		internalState = new HashSet<LongWritable>();
		valueClass = LongWritable.class;
	}
	
	/**
	 * This constructor will return a new LongWritableSet which is an exact copy of the given set.
	 * 
	 * @param toCopy the set to copy.
	 */
	public LongWritableSet(LongWritableSet toCopy){
		internalState = new HashSet<LongWritable>(toCopy.get());
		valueClass = LongWritable.class;
	}
	
}
