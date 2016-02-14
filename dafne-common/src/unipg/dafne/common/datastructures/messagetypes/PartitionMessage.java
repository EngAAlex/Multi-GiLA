package unipg.dafne.common.datastructures.messagetypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class is used during the partitioning label propagation.
 * 
 * @author Alessio Arleo
 *
 */
public class PartitionMessage extends MessageWritable<Long, Short> {

	/**
	 * Parameter-less constructor.
	 * 
	 */
	public PartitionMessage() {
		super();
	}

	/**
	 * Creates a new Partition Message with ttl 0.
	 * 
	 * @param payloadVertex
	 * @param partition
	 */
	public PartitionMessage(long payloadVertex, short partition) {
		super(payloadVertex, partition);
	}
	
	/**
	 * Creates a new Partition Message with ttl 0 with the specified ttl.
	 * 
	 * @param payloadVertex
	 * @param ttl
	 * @param partition
	 */
	public PartitionMessage(long payloadVertex, int ttl, short partition) {
		super(payloadVertex, ttl, partition);
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagate()
	 */
	@Override
	public MessageWritable<Long, Short> propagate() {
		return new PartitionMessage(payloadVertex, ttl-1, value);
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagateAndDie()
	 */
	@Override
	public MessageWritable<Long, Short> propagateAndDie() {
		return new PartitionMessage(payloadVertex, value);
	}
	
	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#specificRead()
	 */
	@Override
	public void specificRead(DataInput input) throws IOException {
		payloadVertex = input.readLong();
		value = input.readShort();
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#specificWrite()
	 */
	@Override
	public void specificWrite(DataOutput output) throws IOException {
		output.writeLong(payloadVertex);
		output.writeShort(value);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		PartitionMessage that = (PartitionMessage) o;
		if (value != that.getValue() || payloadVertex != that.getPayloadVertex()) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return getPayloadVertex() + " " + getValue();
	}

}