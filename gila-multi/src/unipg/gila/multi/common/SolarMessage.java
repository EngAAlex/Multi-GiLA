/**
 * 
 */
package unipg.gila.multi.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import unipg.gila.multi.common.LayeredPartitionedLongWritable;
import unipg.gila.common.datastructures.LinkedListWritable;
import unipg.gila.common.datastructures.messagetypes.MessageWritable;

/**
 * @author Alessio Arleo
 *
 */
public class SolarMessage extends MessageWritable<LayeredPartitionedLongWritable, LayeredPartitionedLongWritable>{
	
	private CODE code;
	private LinkedListWritable<LayeredPartitionedLongWritable> extraPayload;
	
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
	
	public SolarMessage(LayeredPartitionedLongWritable id, int ttl, LayeredPartitionedLongWritable value, LayeredPartitionedLongWritable selectedSun, CODE code){
		super(id, ttl, value);
		this.code = code;
		if(code.equals(CODE.REFUSEOFFER))
			extraPayload = new LinkedListWritable<LayeredPartitionedLongWritable>();
	}
	
	public SolarMessage(LayeredPartitionedLongWritable id, int ttl, LayeredPartitionedLongWritable phySender, LayeredPartitionedLongWritable selectedSun, CODE code, LinkedListWritable<LayeredPartitionedLongWritable> extraPayload){
		super(id, ttl, phySender);
		this.code = code;
		if(code.equals(CODE.REFUSEOFFER))
			this.extraPayload = extraPayload;
	}
	
	public SolarMessage copy() {
		if(!code.equals(CODE.REFUSEOFFER))
			return new SolarMessage(this.payloadVertex.copy(), ttl , this.getValue().copy(), this.code);
		else{
			SolarMessage tmp = new SolarMessage(this.payloadVertex.copy(), ttl , this.getValue().copy(), this.code);
			tmp.copyExtraPayload(extraPayload);
			return tmp;
		}
	}
	
	public void spoofPayloadVertex(LayeredPartitionedLongWritable payloadVertex){
		this.payloadVertex = payloadVertex;
	}
	
	public void addToExtraPayload(LayeredPartitionedLongWritable toAdd){
		if(extraPayload == null){
			extraPayload = new LinkedListWritable<LayeredPartitionedLongWritable>();
		}
		extraPayload.enqueue(toAdd);
	}
	
	@SuppressWarnings("unchecked")
	public void copyExtraPayload(LinkedListWritable<LayeredPartitionedLongWritable> toAdd){
		if(extraPayload == null)
			extraPayload = new LinkedListWritable<LayeredPartitionedLongWritable>(toAdd);
		else
			extraPayload.addAll(toAdd);
		
	}
	
	public LinkedListWritable<LayeredPartitionedLongWritable> getExtraPayload(){
		return extraPayload;
	}
	
	@SuppressWarnings("unchecked")
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
			extraPayload = new LinkedListWritable<LayeredPartitionedLongWritable>();
			extraPayload.readFields(in);
		}
	}

	@Override
	protected void specificWrite(DataOutput out) throws IOException {
		payloadVertex.write(out);
		value.write(out);
		out.writeInt(CODE.write(getCode()));
		if(getCode().equals(CODE.REFUSEOFFER)){
			out.writeInt(extraPayload.size());
			if(extraPayload.size() > 0)
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


	
}

