/**
 * 
 */
package unipg.gila.common.datastructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author spark
 *
 */
public abstract class WritableArray<T> implements Writable {
	
	  protected T[] internalState;

	  
	  public T[] get() {
		    return internalState;
		  }
	  
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException{
		int length = internalState.length;
	    out.writeInt(length);
	    if(length > 0)
	    	specificWrite(length, out);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
	    int length = in.readInt();
	    specificRead(length, in);
	}
	
	
	
	protected abstract void specificRead(int length, DataInput in) throws IOException;
	
	protected abstract void specificWrite(int length, DataOutput out) throws IOException;
	
	@Override
	  public String toString() {
	    String result = "";
	    for (T f : internalState)
	      result += result.equals("") ? f : ", " + f;
	    return result;
	  }
}
