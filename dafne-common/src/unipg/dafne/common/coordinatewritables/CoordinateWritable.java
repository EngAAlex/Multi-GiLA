package unipg.dafne.common.coordinatewritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.json.JSONArray;
import org.json.JSONException;

import unipg.dafne.common.datastructures.LinkedListWritable;
import unipg.dafne.common.datastructures.LongWritableSet;

public class CoordinateWritable extends MiniCoordinateWritable{
	
	protected float fX;
	protected float fY;
	protected boolean justReset; 
	protected LongWritableSet analyzed;
	protected float shortestEdge = Float.MAX_VALUE;
	protected LinkedListWritable messageStack;

	public CoordinateWritable(){
		super();
		fX = 0.0f;
		fY = 0.0f;		
		analyzed = new LongWritableSet();
		justReset = false;
	}
	
	public CoordinateWritable(Float x, Float y, long component){
		super(x, y, component);
		fX = 0.0f;
		fY = 0.0f;			
		analyzed = new LongWritableSet();
		justReset = false;	
	}
	
	public CoordinateWritable(float x, float y,  JSONArray oEs, long component) throws JSONException {
		super(x, y, oEs, component);		
		fX = 0.0f;
		fY = 0.0f;	
		analyzed = new LongWritableSet();
		justReset = false;	
	}
	
	public boolean isAnalyzed(Writable neigh){
		return analyzed.contains(neigh);
	}
	
	public void analyse(LongWritable neigh){
		analyzed.addElement(neigh);
	}
	
	public void resetAnalyzed(){
		analyzed.reset();
		resetForceVector();
		justReset=true;
	}
	
	public void setAsMoving(){
		justReset = false;
	}
	
	public boolean hasBeenReset(){
		return justReset;
	}

	public void setCoordinates(float x, float y){
		this.x = x;
		this.y = y;
	}	
	
	public float[] getCoordinates(){
		return new float[]{x, y};
	}
	
	public void addToForceVector(float[] force){
		this.fX += force[0];
		this.fY += force[1];
	}	
	
	public float[] getForceVector(){
		return new float[]{fX, fY};
	}
	
	public void resetForceVector(){
		this.fX = 0.0f;
		this.fY = 0.0f;
	}
	
	public float getShortestEdge() {
		return shortestEdge;
	}
	
	public void setShortestEdge(float shortestEdge) {
		if(shortestEdge < this.shortestEdge)
			this.shortestEdge = shortestEdge;
	}
	
	public boolean isMessageStackEmpty(){
		return messageStack.isEmpty();
	}
	
	public void enqueueMessage(Writable w){
		if(messageStack == null)
			messageStack = new LinkedListWritable();
		messageStack.enqueue(w);
	}
	
	public Writable[] dequeueAllMessages() {
		return messageStack.flush();
	}
	
	public int messageLeftInStack(){
		if(messageStack == null)
			return 0;
		return messageStack.size();
	}

	public Writable[] dequeueMessages(int messages){
		int capacity = messageStack.isEmpty() ? 0 : Math.min(messages, messageStack.size()); 
		Writable[] result = new Writable[capacity];
		for(int i=0; i<result.length; i++){
			Writable temp = messageStack.dequeue();
			if(temp == null)
				break;
			else
				result[i] = temp;
		}
		return result;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		fX = in.readFloat();
		fY = in.readFloat();
		analyzed.readFields(in);
		if(in.readBoolean())
			messageStack.readFields(in);
		justReset = in.readBoolean();
		shortestEdge = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeFloat(fX);
		out.writeFloat(fY);
		analyzed.write(out);
		if(messageLeftInStack() == 0)
			out.writeBoolean(false);
		else{
			out.writeBoolean(true);
			messageStack.write(out);
		}
		out.writeBoolean(justReset);
		out.writeFloat(shortestEdge);
	}

}

