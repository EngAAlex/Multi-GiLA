package unipg.dafne.common.datastructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * Class that holds the value of each edge in the partitioning phase.
 * 
 * @author claudio
 *
 */
public class EdgeValue implements Writable {
	private short partition = -1;
	private byte weight = 1;

	public EdgeValue() {
	}

	public EdgeValue(EdgeValue o) {
		this(o.getPartition(), o.getWeight());
	}

	public EdgeValue(short partition, byte weight) {
		setPartition(partition);
		setWeight(weight);
	}

	public short getPartition() {
		return partition;
	}

	public void setPartition(short partition) {
		this.partition = partition;
	}

	public byte getWeight() {
		return weight;
	}

	public void setWeight(byte weight) {
		this.weight = weight;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		partition = in.readShort();
		weight = in.readByte();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(partition);
		out.writeByte(weight);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		EdgeValue that = (EdgeValue) o;
		return this.partition == that.partition;
	}

	@Override
	public String toString() {
		return getWeight() + " " + getPartition();
	}
}

