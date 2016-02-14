package unipg.dafne.common.datastructures.messagetypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This kind of message is used to prune the one degree messages and carries the degree of the generating vertex.
 * 
 * @author Alessio Arleo
 *
 */
public class DegMessage extends MessageWritable<Long, Integer> {
	
	private long component;

	/**
	 * Parameter-less constructor.
	 * 
	 */
	public DegMessage() {
		super();
	}

	/**
	 * Constructor setting only payload vertex id and the value of the message, leaving ttl to 0.
	 * 
	 * @param payloadVertex
	 * @param deg
	 */
	public DegMessage(long payloadVertex, int deg) {
		super(payloadVertex, deg);
	}
	
	public DegMessage(int ttl, long payloadVertex, int deg) {
		super(payloadVertex, ttl, deg);
	}
	
	public DegMessage(long payloadVertex, int component, int deg) {
		this(payloadVertex, deg);
		this.component = component;
	}
	
	public DegMessage(long sourceId, int ttl, int component, int deg) {
		this(ttl, sourceId, deg);
		this.component = component;
	}
	
	/**
	 * Returns the component of the vertex who sent this message.
	 * 
	 * @return The connect component id of the vertex who sent this message.
	 */
	public long getComponent() {
		return component;
	}


	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagate()
	 */
	@Override
	public MessageWritable<Long, Integer> propagate() {
		return new DegMessage(payloadVertex, ttl-1, value, value);
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagateAndDie()
	 */
	@Override
	public MessageWritable<Long, Integer> propagateAndDie() {
		return new DegMessage(payloadVertex, value, value);
	}
	
	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#specificRead()
	 */
	@Override
	public void specificRead(DataInput input) throws IOException {
		payloadVertex = input.readLong();
		component = input.readLong();
		value = input.readInt();
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#specificWrite()
	 */
	@Override
	public void specificWrite(DataOutput output) throws IOException {
		output.writeLong(payloadVertex);
		output.writeLong(component);
		output.writeInt(value);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DegMessage that = (DegMessage) o;
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