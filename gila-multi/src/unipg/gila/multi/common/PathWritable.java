package unipg.gila.multi.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class is a data structure used when   
 * 
 * @author Alessio Arleo
 *
 */
public class PathWritable implements Writable {

	private int positionInPath;
	private int pathLength;
	private LayeredPartitionedLongWritable referencedSun;
	
	public PathWritable() {
		referencedSun = new LayeredPartitionedLongWritable();
	}
	
	public PathWritable(int positionInPath, int pathLength, LayeredPartitionedLongWritable ref){
		this.positionInPath = positionInPath;
		this.pathLength = pathLength;
		referencedSun = ref;
	}
	
	public PathWritable copy(){
		return new PathWritable(positionInPath, pathLength, referencedSun.copy());
	}
	
	public int getPositionInpath(){
		return positionInPath;
	}
	
	public int getPathLength(){
		return pathLength;
	}
	
	public LayeredPartitionedLongWritable getReferencedSun(){
		return referencedSun;
	}

	public void readFields(DataInput in) throws IOException {
		positionInPath = in.readInt();
		pathLength = in.readInt();
		referencedSun.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(positionInPath);
		out.writeInt(pathLength);
		referencedSun.write(out);
	}

}
