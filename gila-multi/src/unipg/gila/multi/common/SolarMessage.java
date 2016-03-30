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
package unipg.gila.multi.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

import unipg.gila.multi.common.LayeredPartitionedLongWritable;
import unipg.gila.common.datastructures.messagetypes.MessageWritable;

/**
 * @author Alessio Arleo
 *
 */
public class SolarMessage extends MessageWritable<LayeredPartitionedLongWritable, LayeredPartitionedLongWritable>{
	
	private CODE code;
	private ReferrersList extraPayload;
	
	public static enum CODE{
		SUNOFFER,ACCEPTOFFER,REFUSEOFFER,SUNDISCOVERY,CONFLICT;
		
		public static CODE readFields(int in){
			switch(in){
			case 0: return SUNOFFER; 
			case 1: return ACCEPTOFFER; 
			case 2: return REFUSEOFFER;
			case 3: return CONFLICT;
			default: return SUNDISCOVERY; 
			}
		}
		
		public static int write(CODE code){
			switch(code){
			case SUNOFFER: return 0;
			case ACCEPTOFFER: return 1;
			case REFUSEOFFER: return 2;
			case CONFLICT: return 3;			
			default: return 6;
			}			
		}
	}
		
	public SolarMessage(){
		super();
	}
	
	public SolarMessage(LayeredPartitionedLongWritable payloadVertex, LayeredPartitionedLongWritable value, CODE code) {
		super(payloadVertex, value);
		this.code = code;
	}
	
	public SolarMessage(LayeredPartitionedLongWritable payloadVertex, int ttl, LayeredPartitionedLongWritable value, CODE code) {
		super(payloadVertex, ttl, value);
		this.code = code;
	}
	
	public SolarMessage(LayeredPartitionedLongWritable payload, int ttl, LayeredPartitionedLongWritable valueSun, ReferrersList extraPayload){
		super(payload, ttl, valueSun);
		this.code = CODE.REFUSEOFFER;
		this.extraPayload = extraPayload;
	}
	
	public SolarMessage copy() {
		SolarMessage tmp = new SolarMessage(this.payloadVertex.copy(), ttl , this.getValue().copy(), this.code);
		if(extraPayload != null)
			tmp.copyExtraPayload(extraPayload);
		return tmp;
	}
	
	public void spoofPayloadVertex(LayeredPartitionedLongWritable payloadVertex){
		this.payloadVertex = payloadVertex;
	}
	
	public void addToExtraPayload(LayeredPartitionedLongWritable toAdd){
		if(extraPayload == null)
			extraPayload = new ReferrersList();
		extraPayload.enqueue(toAdd);
	}
	
	public void copyExtraPayload(ReferrersList toAdd){
		if(extraPayload == null)
			extraPayload = new ReferrersList(toAdd);
		extraPayload.addAll(toAdd);
		
	}
	
	public ReferrersList getExtraPayload(){
		return extraPayload;
	}
	
	public Iterator<LayeredPartitionedLongWritable> getExtraPayloadIterator(){
		return (Iterator<LayeredPartitionedLongWritable>) extraPayload.iterator();
	}
	
	public CODE getCode(){
		return code;
	}

	@Override
	protected void specificRead(DataInput in) throws IOException {
		payloadVertex = new LayeredPartitionedLongWritable();
		value = new LayeredPartitionedLongWritable();
		payloadVertex.readFields(in);
		value.readFields(in);
		code = CODE.readFields(in.readInt());
		if(code.equals(CODE.REFUSEOFFER) && in.readInt() > 0){
			extraPayload = new ReferrersList();
			extraPayload.readFields(in);
		}
	}

	@Override
	protected void specificWrite(DataOutput out) throws IOException {
		payloadVertex.write(out);
		value.write(out);
		out.writeInt(CODE.write(getCode()));
		if(getCode().equals(CODE.REFUSEOFFER)){
			int size = extraPayload == null ? 0 : extraPayload.size();
			out.writeInt(size);
			if(size > 0)
				extraPayload.write(out);
		}
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagate()
	 */
	@Override
	public MessageWritable<LayeredPartitionedLongWritable, LayeredPartitionedLongWritable> propagate() {
		if(!code.equals(CODE.REFUSEOFFER))
			return new SolarMessage(getPayloadVertex().copy(), getTTL() - 1, getValue().copy(), getCode());
		else{
			SolarMessage propagatedSolarMessage = new SolarMessage(getPayloadVertex().copy(), getTTL() - 1, getValue().copy(), getCode());
			propagatedSolarMessage.copyExtraPayload(extraPayload);
			return propagatedSolarMessage;
		}
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagateAndDie()
	 */
	@Override
	public MessageWritable<LayeredPartitionedLongWritable, LayeredPartitionedLongWritable> propagateAndDie() {
		if(!code.equals(CODE.REFUSEOFFER))
			return new SolarMessage(getPayloadVertex(), getValue(), getCode());
		else{
			SolarMessage propagatedSolarMessage = new SolarMessage(getPayloadVertex(), getValue(), getCode());
			propagatedSolarMessage.copyExtraPayload(extraPayload);
			return propagatedSolarMessage;
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableFactory#newInstance()
	 */
	public Writable newInstance() {
		return new SolarMessage();
	}


	
}

