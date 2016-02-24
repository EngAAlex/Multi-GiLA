package unipg.gila.common.datastructures.messagetypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * This class represent a message used in the algorithm.
 * 
 * @author Alessio Arleo
 *
 * @param <P> the class of the payload vertex.
 * @param <T> the class of the value carried by the message.
 */
public abstract class MessageWritable<P, T> implements Writable{
	
	protected int ttl;
	protected P payloadVertex;
	protected T value;
	
	/**
	 * Parameter-less constructor.
	 * 
	 */
	public MessageWritable(){
		
	}
	
	/**
	 * Constructor with only payload vertex id and value. TTL is set to zero. 
	 * 
	 * @param payloadVertex
	 * @param value
	 */
	public MessageWritable(P payloadVertex, T value) {
		this.payloadVertex = payloadVertex;
		this.value = value;
		this.ttl = 0;
	}
	
	/**
	 * Constructor setting payload vertex id, value and time to live.
	 * 
	 * @param payloadVertex
	 * @param ttl
	 * @param value
	 */
	public MessageWritable(P payloadVertex, int ttl, T value) {
		this.payloadVertex = payloadVertex;
		this.ttl = ttl;
		this.value = value;
	}
	
	/**
	 * Method that returns the time to live of the message.
	 * 
	 * @return the time to live of the message.
	 */
	public int getTTL(){
		return ttl;
	}

	/**
	 * Returns the payload vertex.
	 * 
	 * @return the payload vertex.
	 */
	public P getPayloadVertex(){ 
		return payloadVertex;
	}

	/**
	 * 
	 * This method propagates the message by creating a new MessageWritable and copying the containing values. 
	 * The returning message will have its time to live diminished.
	 * 
	 * @return A new MessageWritable with a decreased time to live.
	 */
	public abstract MessageWritable<P, T> propagate();

	/**
	 * 
	 * This method propagates the message by creating a new MessageWritable and copying the containing values. 
	 * The returning message will have its time to live zero so that it is scrapped at the next superstep.
	 * 
	 * @return A new MessageWritable with a decreased time to live equal to zero.
	 */
	public abstract MessageWritable<P, T> propagateAndDie();
	/**
	 * 
	 * Method to check if the message should survive or be scrapped.
	 * 
	 * @return
	 */
	public boolean isAZombie(){
		return ttl == 0;
	}

	public void setValue(T value){	
		this.value = value;
	}
	
	public T getValue(){
		return this.value;
	}

	public void readFields(DataInput in) throws IOException{
		ttl = in.readInt();
		specificRead(in);
	}
	
	public void write(DataOutput out) throws IOException{
		out.writeInt(ttl);
		specificWrite(out);
	}
	
	/**
	 * Method that guarantees that extending classes are serialized properly. All instance variables (excluding the ttl) must be serialized in this method 
	 * (payloadVertex, value and any other variable introduced). To know more about the serializing process in Hadoop please visit <a href='https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/io/Writable.html'>this</a> website. 
	 * 
	 *  
	 * @param DataInput
	 * @throws IOException
	 */
	protected  abstract void specificRead(DataInput in) throws IOException;
	
	/**
	 * Method that guarantees that extending classes are de-serialized properly. All instance variables (excluding the ttl) must be de-serialized in this method 
	 * (payloadVertex, value and any other variable introduced). To know more about the serializing process in Hadoop please visit <a href='https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/io/Writable.html'>this</a> website.
	 * 
	 * @param DataOutput
	 * @throws IOException
	 */
	protected abstract void specificWrite(DataOutput out) throws IOException;

}

