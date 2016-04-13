/**
 * 
 */
package unipg.gila.multi.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * @author Alessio Arleo
 *
 */
public class Referrer implements Writable {

	LayeredPartitionedLongWritable eventGenerator;
	int distanceAccumulator = 0;
	
	/**
	 * 
	 */
	public Referrer() {
		eventGenerator = new LayeredPartitionedLongWritable();
	}
	
	/**
	 * 
	 */
	public Referrer(LayeredPartitionedLongWritable eventG, int distance) {
		eventGenerator = eventG;
		distanceAccumulator = distance;
	}
	
	
	
	/**
	 * @return the eventGenerator
	 */
	public LayeredPartitionedLongWritable getEventGenerator() {
		return eventGenerator;
	}



	/**
	 * @return the distanceAccumulator
	 */
	public int getDistanceAccumulator() {
		return distanceAccumulator;
	}



	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		eventGenerator.write(out);
		out.writeInt(distanceAccumulator);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		eventGenerator.readFields(in);
		distanceAccumulator = in.readInt();
	}

}
