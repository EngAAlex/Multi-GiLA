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
package unipg.gila.common.datastructures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A class representing an array of floats implementing the Writable interface.
 * 
 * @author Alessio Arleo
 *
 */
public class DoubleWritableArray extends WritableArray<Double> {

	public DoubleWritableArray() {
	}

	public DoubleWritableArray(double[] in) {
		internalState = new Double[in.length];
		for (int i = 0; i < in.length; i++)
			internalState[i] = in[i];
	}

	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.WritableArray#specificRead(int, java.io.DataInput)
	 */
	@Override
	protected void specificRead(int length, DataInput in) throws IOException {
		internalState = new Double[length];
		for (int i = 0; i < length; i++)
			internalState[i] = in.readDouble();	
	}

	/* (non-Javadoc)
	 * @see unipg.gila.common.datastructures.WritableArray#specificWrite(int, java.io.DataOutput)
	 */
	@Override
	protected void specificWrite(int length, DataOutput out) throws IOException {
		for (int i = 0; i < internalState.length; i++)
			out.writeDouble(internalState[i]);	
	}

}

