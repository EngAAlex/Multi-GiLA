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
