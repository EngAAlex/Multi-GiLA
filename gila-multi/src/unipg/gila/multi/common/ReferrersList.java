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
public class ReferrersList extends LinkedListWritable<Referrer>{

	public ReferrersList(){
		internalState = new LinkedList<Referrer>();
	}
	
	public ReferrersList(LinkedListWritable<Referrer> toCopy){
		this();
		if(toCopy != null && toCopy.size() > 0)
			addAll(toCopy);
	}

	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.LinkedListWritable#specificRead(java.io.DataInput)
	 */
	@Override
	protected Referrer specificRead(DataInput in) throws IOException{
		Referrer current = new Referrer();
		current.readFields(in);
		return current;
	}
	
	
	
}
