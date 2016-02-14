/**
 * 
 */
package unipg.dafne.common.datastructures.messagetypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class is used to carry the coordinates of the generating vertex across the graph as an array of floats.
 * 
 * @author Alessio Arleo
 *
 */
public class PlainMessage extends MessageWritable<Long, float[]> {

	/**
	 * Parameter-less constructor
	 * 
	 */
	public PlainMessage() {
		super();
	}
	
	/**
	 * Creates a new PlainMessage with ttl 0.
	 * 
	 * @param payloadVertex
	 * @param coords
	 */
	public PlainMessage(long payloadVertex, float[] coords){
		super(payloadVertex, coords);		
	}
	
	/**
	 * Creates a new PlainMessage with the given ttl.
	 * 
	 * @param payloadVertex
	 * @param ttl
	 * @param coords
	 */
	public PlainMessage(long payloadVertex, int ttl, float[] coords){
		super(payloadVertex, ttl, coords);		
	}
	
	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagate()
	 */
	@Override
	public MessageWritable<Long, float[]> propagate() {
		return new PlainMessage(payloadVertex, ttl-1, new float[]{value[0], value[1]});
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagateAndDie()
	 */
	@Override
	public MessageWritable<Long, float[]> propagateAndDie() {
		return new PlainMessage(payloadVertex, new float[]{value[0], value[1]});
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#specificRead(java.io.DataInput)
	 */
	@Override
	protected void specificRead(DataInput in) throws IOException {
		payloadVertex = in.readLong();
		value = new float[2];
		value[0] = in.readFloat();
		value[1] = in.readFloat();
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#specificWrite(java.io.DataOutput)
	 */
	@Override
	protected void specificWrite(DataOutput out) throws IOException {
		out.writeLong(payloadVertex);
		out.writeFloat(value[0]);
		out.writeFloat(value[1]);
	}

}
