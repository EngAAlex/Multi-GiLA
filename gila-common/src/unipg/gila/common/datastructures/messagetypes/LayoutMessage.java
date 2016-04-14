/*******************************************************************************
 * Copyright 2016 Alessio Arleo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
 * This class is used to carry the coordinates of the generating vertex across the graph as an array of floats.
 * 
 * @author Alessio Arleo
 *
 */
public class LayoutMessage extends MessageWritable<PartitionedLongWritable, float[]> {

//	private int deg;
	
	/**
	 * Parameter-less constructor
	 * 
	 */
	public LayoutMessage() {
		super();
	}
	
	/**
	 * Creates a new PlainMessage with ttl 0.
	 * 
	 * @param payloadVertex
	 * @param coords
	 */
	public LayoutMessage(PartitionedLongWritable payloadVertex, float[] coords){
		super(payloadVertex, coords);		
	}
	
	/**
	 * Creates a new PlainMessage with the given ttl.
	 * 
	 * @param payloadVertex
	 * @param ttl
	 * @param coords
	 */
	public LayoutMessage(PartitionedLongWritable payloadVertex, int ttl, float[] coords){
		super(payloadVertex, ttl, coords);		
	}
	
//	public void setDeg(int deg){
//		this.deg = deg;
//	}
//
//	public int getDeg(){
//		return deg;
//	}
	
	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagate()
	 */
	@Override
	public MessageWritable<PartitionedLongWritable, float[]> propagate() {
		LayoutMessage toReturn = new LayoutMessage(payloadVertex, ttl-1, new float[]{value[0], value[1]});
		toReturn.setWeight(weight);
//		if(getDeg() != -1)
//			toReturn.setDeg(getDeg());
		return toReturn;
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagateAndDie()
	 */
	@Override
	public MessageWritable<PartitionedLongWritable, float[]> propagateAndDie() {
		LayoutMessage toReturn = new LayoutMessage(payloadVertex, new float[]{value[0], value[1]});
		toReturn.setWeight(weight);
		return toReturn;
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#specificRead(java.io.DataInput)
	 */
	@Override
	protected void specificRead(DataInput in) throws IOException {
		payloadVertex = new PartitionedLongWritable();
		payloadVertex.readFields(in);
		value = new float[2];
		value[0] = in.readFloat();
		value[1] = in.readFloat();
//		if(in.readBoolean())
//			deg = in.readInt();
			
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#specificWrite(java.io.DataOutput)
	 */
	@Override
	protected void specificWrite(DataOutput out) throws IOException {
		payloadVertex.write(out);
		out.writeFloat(value[0]);
		out.writeFloat(value[1]);
//		if(getDeg() == -1)
//			out.writeBoolean(false);
//		else{
//			out.writeBoolean(true);
//			out.writeInt(deg);
//		}
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
	public MessageWritable copy() {
		return new LayoutMessage(payloadVertex, ttl, value);
	}
	

}
