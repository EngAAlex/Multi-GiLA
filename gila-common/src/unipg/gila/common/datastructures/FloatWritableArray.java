package unipg.gila.common.datastructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * A class representing an array of floats implementing the Writable interface.
 * 
 * @author Alessio Arleo
 *
 */
public class FloatWritableArray implements Writable {

	private float[] internalState;
	
	public FloatWritableArray() {
	}
	
	public FloatWritableArray(float[] in){
		internalState = new float[in.length];
		for(int i=0; i<in.length; i++)
			internalState[i] = in[i];
	}
	
	public float[] get(){
		return internalState;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		internalState = new float[length];
		for(int i=0; i<length; i++)
			internalState[i] = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(internalState.length);
		for(int i=0; i<internalState.length; i++)
			out.writeFloat(internalState[i]);
	}

	@Override
	public String toString() {
		String result = "";
		for(float f : internalState)
			result += result.equals("") ? f : ", " + f;
		return result;
	}


}
