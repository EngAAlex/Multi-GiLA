package unipg.gila.multi.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import unipg.gila.common.datastructures.PartitionedLongWritable;

/**
 * @author Alessio Arleo
 *
 */
public class LayeredPartitionedLongWritable extends PartitionedLongWritable {
	
	protected int layer = 0;
	
	public LayeredPartitionedLongWritable() {
		super();
	}

	public LayeredPartitionedLongWritable(String id) {
		super(id);
	}
	
	public LayeredPartitionedLongWritable(short partition, long id){
		super(partition, id);
	}
	
	public LayeredPartitionedLongWritable(short partition, long id, int layer){
		super(partition, id);
		this.layer = layer;
	}
	
	public LayeredPartitionedLongWritable(LayeredPartitionedLongWritable idToCopy) {
		this(idToCopy.getPartition(), idToCopy.getId(), idToCopy.getLayer());
	}

	public LayeredPartitionedLongWritable copy(){
		return new LayeredPartitionedLongWritable(partition, id, layer);
	}
	
	public LayeredPartitionedLongWritable getAdjacentLayerID(int offset){
		return new LayeredPartitionedLongWritable(getPartition(), getId(), getLayer()+offset);
	}
	
	public int getLayer() {
		return layer;
	}

	public void setLayer(int layer) {
		this.layer = layer;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		layer = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(layer);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LayeredPartitionedLongWritable other = (LayeredPartitionedLongWritable) o;
		if (this.partition == other.getPartition() && this.id == other.getId() && this.layer == other.getLayer()) {
			return true;
		}
		return false;
	}

	@Override
	public String toString() {
		return partition + DELIMITER + id + DELIMITER + layer;
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public int compareTo(Object o) {
		if (o == this) {
			return 0;
		}
		LayeredPartitionedLongWritable other = (LayeredPartitionedLongWritable) o;
		if(layer != other.getLayer())
			return this.layer > other.getLayer() ? +1 : this.layer < other.getLayer() ? -1 : 0;

		return this.id > other.id ? +1 : this.id < other.id ? -1 : 0;
	}
}
