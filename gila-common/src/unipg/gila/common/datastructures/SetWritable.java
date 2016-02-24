package unipg.gila.common.datastructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

/**
 * Implementation of a Set implementing the Writable (org.apache.hadoop.io.Writable) interface.
 * 
 * @author Alessio Arleo
 *
 * @param <P> The class of the object contained in the set. Must implement Writable.
 */
public class SetWritable<P extends Writable> implements Writable {
	
	protected Set<P> internalState;
	protected Class<P> valueClass;
	
	/**
	 * 
	 */
	public SetWritable() {
		internalState = new HashSet<P>();
	}
	
	public SetWritable(Collection<P> cll){
		this();
		addAll(cll);
	}
		
	public void addAll(Collection<P> it){
		internalState.addAll(it);
	}
	
	public Set<P> get(){
		return internalState;
	}
	
	public void reset(){
		internalState.clear();
	}
	
	public void addElement(P element){
		internalState.add(element);
	}
	
	public boolean contains(Writable key){
		return internalState.contains(key);
	}
	
	public boolean remove(Writable elementToRemove){
		return internalState.remove(elementToRemove);
	}

	public int size(){
		return internalState.size();
	}
	
	public Iterator<? extends Writable> iterator(){
		return internalState.iterator();
	}

	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		internalState.clear();
		int limit = in.readInt();
	    for (int i = 0; i < limit; i++) {
	      P value = (P) WritableFactories.newInstance(valueClass);
	      value.readFields(in);                       
	      internalState.add(value);                         
	    }
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(internalState.size());
		Iterator<P> it = internalState.iterator();
		while(it.hasNext())
			it.next().write(out);
	}

}	
