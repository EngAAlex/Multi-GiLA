package unipg.gila.common.datastructures.messagetypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import unipg.gila.common.multi.LayeredPartitionedLongWritable;


/**
 * @author Alessio Arleo
 *
 */
public class LayoutMessage extends LayoutMessageMatrix<LayeredPartitionedLongWritable> {

	/**
	 * Parameter-less constructor. 
	 */
	public LayoutMessage() {

	}
	
	/**
	 * Creates a new PlainMessage with ttl 0.
	 * 
	 * @param payloadVertex
	 * @param coords
	 */
	public LayoutMessage(LayeredPartitionedLongWritable payloadVertex, float[] coords){
		super(payloadVertex, coords);		
	}

	/**
	 * Creates a new PlainMessage with the given ttl.
	 * 
	 * @param payloadVertex
	 * @param ttl
	 * @param coords
	 */
	public LayoutMessage(LayeredPartitionedLongWritable payloadVertex, int ttl, float[] coords){
		super(payloadVertex, ttl, coords);		
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagate()
	 */
	@Override
	public MessageWritable<LayeredPartitionedLongWritable, float[]> propagate() {
		LayoutMessage toReturn = new LayoutMessage(payloadVertex, ttl-1, new float[]{value[0], value[1]});
		toReturn.setWeight(weight);
		//			if(getDeg() != -1)
		//				toReturn.setDeg(getDeg());
		return toReturn;
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagateAndDie()
	 */
	@Override
	public MessageWritable<LayeredPartitionedLongWritable, float[]> propagateAndDie() {
		LayoutMessage toReturn = new LayoutMessage(payloadVertex, new float[]{value[0], value[1]});
		toReturn.setWeight(weight);
		return toReturn;
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#specificRead(java.io.DataInput)
	 */
	@Override
	protected void specificRead(DataInput in) throws IOException {
		payloadVertex = new LayeredPartitionedLongWritable();
		payloadVertex.readFields(in);		
		value = new float[2];
		value[0] = in.readFloat();
		value[1] = in.readFloat();
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#specificWrite(java.io.DataOutput)
	 */
	@Override
	protected void specificWrite(DataOutput out) throws IOException {
		payloadVertex.write(out);
		out.writeFloat(value[0]);
		out.writeFloat(value[1]);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableFactory#newInstance()
	 */
	public Writable newInstance() {
		return new LayoutMessage();
	}

	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.messagetypes.MessageWritable#copy()
	 */
	@Override
	public LayoutMessage copy() {
		return new LayoutMessage(payloadVertex.copy(), ttl, value);
	}
	
	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.messagetypes.MessageWritable#toString()
	 */
	@Override
	public String toString() {
		return payloadVertex.toString()+ " " + getValue().toString() + " ttl " + getTTL() + " weight " + weight;
	}


}


