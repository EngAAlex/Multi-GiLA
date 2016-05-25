/*******************************************************************************
 * Copyright 2016 Alessio Arleo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
public abstract class SetWritable<P extends Writable> implements Writable {
	
	
	protected Set<P> internalState;	
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
	
	public void addAll(SetWritable<P> in){
		internalState.addAll(in.get());
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

	public void readFields(DataInput in) throws IOException {
		internalState.clear();
		int limit = in.readInt();
	    for (int i = 0; i < limit; i++) {
	    	internalState.add(specificRead(in));                       
	    }
	}

	/**
	 * This method is used to let the subclasses properly deserialize the data from the DataInput.  
	 * 
	 * @param in The DataInput object from which read the data.
	 * @throws IOException 
	 */
	protected abstract P specificRead(DataInput in) throws IOException;
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(internalState.size());
		Iterator<P> it = internalState.iterator();
		while(it.hasNext())
			it.next().write(out);
	}

	/**
	 * 
	 */
	public void clear() {
		internalState.clear();
	}

}	
