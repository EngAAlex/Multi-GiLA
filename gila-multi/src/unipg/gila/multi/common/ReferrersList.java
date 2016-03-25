/**
 * 
 */
package unipg.gila.multi.common;

import java.io.DataInput;
import java.io.IOException;
import java.util.LinkedList;

import unipg.gila.common.datastructures.LinkedListWritable;

/**
 * @author Alessio Arleo
 *
 */
public class ReferrersList extends LinkedListWritable<LayeredPartitionedLongWritable>{

	public ReferrersList(){
		internalState = new LinkedList<LayeredPartitionedLongWritable>();
	}
	
	public ReferrersList(LinkedListWritable<LayeredPartitionedLongWritable> toCopy){
		this();
		if(toCopy != null && toCopy.size() > 0)
			addAll(toCopy);
	}

	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.LinkedListWritable#specificRead(java.io.DataInput)
	 */
	@Override
	protected LayeredPartitionedLongWritable specificRead(DataInput in) throws IOException{
		LayeredPartitionedLongWritable current = new LayeredPartitionedLongWritable();
		current.readFields(in);
		return current;
	}
	
	
	
}
