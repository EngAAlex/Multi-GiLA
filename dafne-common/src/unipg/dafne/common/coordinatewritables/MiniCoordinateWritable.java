package unipg.dafne.common.coordinatewritables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.json.JSONArray;
import org.json.JSONException;

import unipg.dafne.common.datastructures.LongWritableSet;

public class MiniCoordinateWritable implements Writable{

	protected float x;
	protected float y;
	protected LongWritableSet oneEdges;
	protected long component;

	public MiniCoordinateWritable() {
		x = 0.0f;
		y = 0.0f;
		component = -1;
	}
	
	public MiniCoordinateWritable(float x, float y, long component){
		this.x = x;
		this.y = y;
		this.component = component;
	}

	public MiniCoordinateWritable(float x, float y, JSONArray oEs, long component) throws JSONException{
		this(x,y,component);
		oneEdges = new LongWritableSet();
		
		for(int i=0; i<oEs.length(); i++)
			oneEdges.addElement(new LongWritable(oEs.getLong(i)));
	}

	public float[] getCoordinates(){
		return new float[]{x, y};
	}
	
	public void setCoordinates(float x, float y) {
		this.x = x;
		this.y = y;
	}
	
	public int getOneDegreeVerticesQuantity() {
		if(oneEdges == null)
			return 0;
		return oneEdges.size();
	}

	@SuppressWarnings("unchecked")
	public Iterator<LongWritable> getOneDegreeVertices(){
		return (Iterator<LongWritable>) oneEdges.iterator();
	}
		
	public long getComponent() {
		return component;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readFloat();
		y = in.readFloat();
		if(in.readBoolean()){
			oneEdges = new LongWritableSet();
			oneEdges.readFields(in);
		}
		component = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(x);
		out.writeFloat(y);
		if(getOneDegreeVerticesQuantity() == 0)
			out.writeBoolean(false);
		else{
			out.writeBoolean(true);
			oneEdges.write(out);
		}
		out.writeLong(component);
	}

}