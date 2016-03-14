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
package unipg.gila.common.coordinatewritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.json.JSONArray;
import org.json.JSONException;

import unipg.gila.common.datastructures.LinkedListWritable;
import unipg.gila.common.datastructures.LongWritableSet;

/**
 * This class models the vertex value.
 * @author Alessio Arleo
 *
 */
public class CoordinateWritable extends MiniCoordinateWritable{
	
	/**
	 * The X component of the forces acting on the vertex.
	 */
	protected float fX;
	/**
	 * The Y component of the forces acting on the vertex.
	 */
	protected float fY;
	/**
	 * A switch used to know if the vertex has been reset or not; it is used to check if whether to 
	 * include the attractive forces or not at the next propagator step.
	 */
	protected boolean justReset; 
	/**
	 * A set which contains all the ids of the vertices already taken into account.
	 */
	protected LongWritableSet analyzed;
	/**
	 * The shortest incident edge.
	 */
	protected float shortestEdge = Float.MAX_VALUE;
	protected LinkedListWritable<Writable> messageStack;

	public CoordinateWritable(){
		super();
		fX = 0.0f;
		fY = 0.0f;		
		analyzed = new LongWritableSet();
		justReset = false;
	}
	
	public CoordinateWritable(Float x, Float y, int component){
		super(x, y, component);
		fX = 0.0f;
		fY = 0.0f;			
		analyzed = new LongWritableSet();
		justReset = false;	
	}
	
	public CoordinateWritable(float x, float y,  JSONArray oEs, int component) throws JSONException {
		super(x, y, oEs, component);		
		fX = 0.0f;
		fY = 0.0f;	
		analyzed = new LongWritableSet();
		justReset = false;	
	}
	
	public boolean isAnalyzed(Writable neigh){
		return analyzed.contains(neigh);
	}
	
	/**
	 * Set a new vertex as analyzed.
	 * 
	 * @param neigh The vertex id to be set as analyzed.
	 */
	public void analyze(LongWritable neigh){
		analyzed.addElement(neigh);
	}
	
	/**
	 * At the end of a drawing cycle (seeding + propagation) this method resets the Analyzed set.
	 */
	public void resetAnalyzed(){
		analyzed.reset();
		resetForceVector();
		justReset=true;
	}
	
	/**
	 * When called, it means that for that vertex attractive forces have been already computed.
	 */
	public void setAsMoving(){
		justReset = false;
	}
	
	/**
	 * Returns true if the vertex has been reset (resetAnalyzed has been called).
	 * @return
	 */
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
	
	/**
	 * Adds to the internal force vector the values in the given 2-dimensional array.
	 * @param force The array to add to the internal force array.
	 */
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

