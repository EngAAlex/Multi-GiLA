package unipg.gila.common.datastructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * This class holds the Vertex value during the partitioning phase.
 * 
 * @author Alessio Arleo
 * @author claudio
 *
 */
public class PartitioningVertexValue implements Writable {
	private short currentPartition = -1;
	private short newPartition = -1;
	private float[] coords;
	private LongWritableSet oneEdges;
	private long component = -1;
	
	public PartitioningVertexValue() {
	}
	
	public PartitioningVertexValue(float[] coords) {
		this();
		this.coords = coords;
	}

	public short getCurrentPartition() {
		return currentPartition;
	}

	public void setCurrentPartition(short p) {
		currentPartition = p;
	}

	public short getNewPartition() {
		return newPartition;
	}

	public void setNewPartition(short p) {
		newPartition = p;
	}	

	public long getComponent() {
		return component;
	}

	public void setComponent(long component) {
		this.component = component;
	}
	
	public void addOneEdge(long id){
		if(oneEdges == null)
			oneEdges = new LongWritableSet();
		oneEdges.addElement(new LongWritable(id));
	}
	
	@SuppressWarnings("unchecked")
	public Iterator<LongWritable> getOneEdges(){
		if(getOneEdgesNo() == 0)
			return null;
		return (Iterator<LongWritable>) oneEdges.iterator();
	}
	
	public int getOneEdgesNo() {
		if(oneEdges != null)
			return oneEdges.size();
		return 0;
	}

	public float[] getCoords() {
		return coords;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		currentPartition = in.readShort();
		newPartition = in.readShort();
		component = in.readLong();
		if(in.readBoolean()){
			oneEdges = new LongWritableSet();
			oneEdges.readFields(in);
		}
		if(in.readBoolean())
			coords = new float[]{in.readFloat(), in.readFloat()};
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(currentPartition);
		out.writeShort(newPartition);
		out.writeLong(component);
		if(getOneEdgesNo() == 0)
			out.writeBoolean(false);
		else
			oneEdges.write(out);
		if(coords == null)
			out.writeBoolean(false);
		else{
			out.writeBoolean(true);
			out.writeFloat(coords[0]);
			out.writeFloat(coords[1]);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		PartitioningVertexValue that = (PartitioningVertexValue) o;
		if (currentPartition != that.currentPartition
				|| newPartition != that.newPartition) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return getCurrentPartition() + " " + getNewPartition();
	}

}
