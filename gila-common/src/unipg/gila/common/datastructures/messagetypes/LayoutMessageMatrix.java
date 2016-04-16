/**
 * 
 */
package unipg.gila.common.datastructures.messagetypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import unipg.gila.common.datastructures.PartitionedLongWritable;

/**
 * @author Alessio Arleo
 *
 */
public class LayoutMessageMatrix<I extends PartitionedLongWritable> extends MessageWritable<I, float[]> {
	
	/**
	 * 
	 */
	public LayoutMessageMatrix() {
		super();
	}
	
	/**
	 * @param payloadVertex
	 * @param coords
	 */
	public LayoutMessageMatrix(I payloadVertex,
			float[] coords) {
		super(payloadVertex, coords);
	}
	
	public LayoutMessageMatrix(I payloadVertex, int ttl,
			float[] coords) {
		super(payloadVertex, ttl, coords);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableFactory#newInstance()
	 */
	public Writable newInstance() {
		return null;
	}

	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.messagetypes.MessageWritable#propagate()
	 */
	@Override
	public MessageWritable<I, float[]> propagate() {
		return null;
	}

	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.messagetypes.MessageWritable#propagateAndDie()
	 */
	@Override
	public MessageWritable<I, float[]> propagateAndDie() {
		return null;
	}

	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.messagetypes.MessageWritable#specificRead(java.io.DataInput)
	 */
	@Override
	protected void specificRead(DataInput in) throws IOException {

	}

	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.messagetypes.MessageWritable#specificWrite(java.io.DataOutput)
	 */
	@Override
	protected void specificWrite(DataOutput out) throws IOException {

	}

	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.messagetypes.MessageWritable#copy()
	 */
	@Override
	public LayoutMessageMatrix<I> copy() {
		// TODO Auto-generated method stub
		return null;
	}

}
