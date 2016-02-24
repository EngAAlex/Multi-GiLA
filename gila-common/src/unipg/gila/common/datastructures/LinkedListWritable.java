package unipg.gila.common.datastructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;

/**
 * A class representing a Linked List implementing the Writable interface.
 * 
 * @author Alessio Arleo
 *
 */
public class LinkedListWritable implements Writable {

	private LinkedList<Writable> internalState;
	
	public LinkedListWritable(){
		internalState = new LinkedList<Writable>();
	}
	
	public void enqueue(Writable toEnqueue){
		internalState.addLast(toEnqueue);
	}
	
	public Writable dequeue(){
		if(!isEmpty())
			return internalState.pop();
		else return null;
	}
	
	public int size(){
		return internalState.size();
	}
	
	public boolean isEmpty(){
		return internalState.isEmpty();
	}
	
	public Writable[] flush() {
		Writable[] result = internalState.toArray(new Writable[0]);
		internalState.clear();
		return result;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
	    internalState.clear();

	    int numFields = in.readInt();
	    if (numFields == 0)
	      return;
	    String className = in.readUTF();
	    Writable obj;
	    try {
	      Class<Writable> c = (Class<Writable>) Class.forName(className);
	      for (int i = 0; i < numFields; i++) {
	        obj = (Writable) c.newInstance();
	        obj.readFields(in);
	        internalState.add(obj);
	      }

	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	}

	@Override
	public void write(DataOutput out) throws IOException {
	   int size = internalState.size();
		out.writeInt(size);
	    if (size == 0)
	      return;
	    Writable obj = internalState.get(0);

	    out.writeUTF(obj.getClass().getCanonicalName());

	    for (int i = 0; i < size; i++) {
	      obj = internalState.get(i);
	      if (obj == null) {
	        throw new IOException("Cannot serialize null fields!");
	      }
	      obj.write(out);
	    }
	}

}
