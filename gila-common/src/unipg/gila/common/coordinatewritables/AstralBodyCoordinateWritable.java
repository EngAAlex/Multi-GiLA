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
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.json.JSONArray;
import org.json.JSONException;

import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.common.multi.LayeredPartitionedLongWritableSet;
import unipg.gila.common.multi.PathWritable;
import unipg.gila.common.multi.PathWritableSet;
import unipg.gila.common.multi.Referrer;
import unipg.gila.common.multi.ReferrersList;

/**
 * @author Alessio Arleo
 *
 */
public class AstralBodyCoordinateWritable extends CoordinateWritable {

	// LOGGER
	// Logger log = Logger.getLogger(AstralBodyCoordinateWritable.class);

	protected LayeredPartitionedLongWritable favProxy;
	protected LayeredPartitionedLongWritableSet sunProxies;
	protected LayeredPartitionedLongWritable sun;
	protected int distanceFromSun = -1;
	protected int weightFromSun = -1;
	protected MapWritable planets; // USED WHEN A BODY IS A SUN
	protected MapWritable moons; // USED WHEN A BODY IS A SUN
	protected MapWritable neighborSystems;
	protected int lowerLevelWeight = 1;
	protected int astralWeight = 1;
	protected boolean cleared = false;
	protected boolean assigned = false;

	public AstralBodyCoordinateWritable() {
		super();
		// sunProxy = new LayeredPartitionedLongWritable();
		// sun = new LayeredPartitionedLongWritable();
	}

	public AstralBodyCoordinateWritable(double x, double y, JSONArray oEs,
			int component) throws JSONException {
		super(x, y, oEs, component);
		// sunProxy = new LayeredPartitionedLongWritable();
		// sun = new LayeredPartitionedLongWritable();
	}

	public AstralBodyCoordinateWritable(int lowerLevelWeigth, double x, double y, JSONArray oEs,
			int component) throws JSONException {
		super(x, y, oEs, component);
		this.lowerLevelWeight = lowerLevelWeigth;
		// sunProxy = new LayeredPartitionedLongWritable();
		// sun = new LayeredPartitionedLongWritable();
	}

	public AstralBodyCoordinateWritable(int lowerLevelWeigth, double x, double y,
			int component) {
		super(x, y, component);
		// sunProxy = new LayeredPartitionedLongWritable();
		// sun = new LayeredPartitionedLongWritable();
		this.lowerLevelWeight = lowerLevelWeigth;
	}

	public AstralBodyCoordinateWritable(double x, double y, int component) {
		super(x, y, component);
		// sunProxy = new LayeredPartitionedLongWritable();
		// sun = new LayeredPartitionedLongWritable();
	}

	public int astralWeight() {
		return lowerLevelWeight + astralWeight;
	}

	public int getLowerLevelWeight() {
		return lowerLevelWeight;
	}

	// public LayeredPartitionedLongWritable getProxy(){
	// return sunProxy;
	// }

	public int getDistanceFromSun() {
		return distanceFromSun;
	}

	/**
	 * @return the weightFromSun
	 */
	public int getWeightFromSun() {
		return weightFromSun;
	}

	/**
	 * @param weightFromSun
	 *          the weightFromSun to set
	 */
	public void setWeightFromSun(int weightFromSun) {
		this.weightFromSun = weightFromSun;
	}

	public void setAsSun() {
		distanceFromSun = 0;
		weightFromSun = 0;
		planets = new MapWritable();
		moons = new MapWritable();
		neighborSystems = new MapWritable();
	}

	public void setAsPlanet(int weightFromSun) {
		distanceFromSun = 1;
		this.weightFromSun = weightFromSun;
	}

	public void setAsMoon(int weightFromSun) {
		distanceFromSun = 2;
		this.weightFromSun = weightFromSun;
	}

	public void setAssigned() {
		assigned = true;
	}

	/**
	 * 
	 */
	public void resetAssigned() {
		assigned = false;
	}

	public boolean isAssigned() {
		return assigned;
	}

	public boolean isAsteroid() {
		return distanceFromSun == -1;
	}

	/**
	 * @return
	 */
	public boolean isPlanet() {
		return distanceFromSun == 1;
	}

	public boolean isMoon() {
		return distanceFromSun > 1;
	}

	public boolean isSun() {
		return distanceFromSun == 0;
	}

	public void resetToAsteroid() {
		distanceFromSun = -1;
	}
	
	public void addPlanet(LayeredPartitionedLongWritable id){
		if (planets == null)
			planets = new MapWritable();
		planets.put(id.copy(), new PathWritableSet());		
	}

	public void addPlanet(LayeredPartitionedLongWritable id, int weight) {
		addPlanet(id);
		astralWeight += weight;
		// log.info("Me, sun accept as a planet the vertex " + id);
	}
	
	public void addMoon(LayeredPartitionedLongWritable id) {
		if (moons == null)
			moons = new MapWritable();
		moons.put(id.copy(), new PathWritableSet());
	}

	public void addMoon(LayeredPartitionedLongWritable id, int weight) {
		addMoon(id);
		astralWeight += weight;
		// log.info("Me, sun accept as a moon the vertex " + id);
	}

	public void addNeighbourSystem(LayeredPartitionedLongWritable sun,
			int weight){
		if (neighborSystems == null)
			neighborSystems = new MapWritable();
		if (neighborSystems.containsKey(sun)) {
			if (weight > ((IntWritable) neighborSystems.get(sun)).get())
				neighborSystems.put(sun, new IntWritable(weight));
		} else
			neighborSystems.put(sun, new IntWritable(weight));	  
	}

	public void addNeighbourSystem(LayeredPartitionedLongWritable sun,
			ReferrersList referrers, int weight) {
		if (neighborSystems == null)
			neighborSystems = new MapWritable();
		if (neighborSystems.containsKey(sun)) {
			if (weight > ((IntWritable) neighborSystems.get(sun)).get())
				neighborSystems.put(sun, new IntWritable(weight));
		} else
			neighborSystems.put(sun, new IntWritable(weight));

		if (referrers == null)
			return;
		Iterator<Referrer> it = (Iterator<Referrer>) referrers.iterator();
		while (it.hasNext()) {
			Referrer currentReferrer = it.next();
			int currentDistanceAccumulator = currentReferrer.getDistanceAccumulator();
			int difference = weight - currentDistanceAccumulator;
			PathWritable tmp = new PathWritable(difference, sun);
			LayeredPartitionedLongWritable eventGenerator = currentReferrer.getEventGenerator();
			addPathWritableToSet(eventGenerator, tmp);
//			PathWritableSet pSet = null;
//			if (planets.containsKey(eventGenerator)) {
//				pSet = ((PathWritableSet) planets.get(eventGenerator));
//			} else {
//				if (moons != null) {
//					pSet = (PathWritableSet) moons.get(eventGenerator);
//				}
//			}
//			if(pSet != null && !pSet.contains(tmp))
//				pSet.addElement(tmp);
		}
	}
	
	public void addPathWritableToSet(LayeredPartitionedLongWritable referrer, PathWritable pw){
		PathWritableSet pSet = null;
		if (planets.containsKey(referrer)) {
			pSet = ((PathWritableSet) planets.get(referrer));
		} else {
			if (moons != null) {
				pSet = (PathWritableSet) moons.get(referrer);
			}			
		}
		if(pSet != null && !pSet.contains(pw))
			pSet.addElement(pw);
		
	}

	public int getPathLengthForNeighbor(LayeredPartitionedLongWritable neighbor) {
		return ((IntWritable) neighborSystems.get(neighbor)).get();
	}

	public Iterator<Entry<Writable, Writable>> getPlanetsIterator() {
		if (planets == null || planets.size() == 0)
			return null;
		return planets.entrySet().iterator();
	}

	public Iterator<Entry<Writable, Writable>> getMoonsIterator() {
		if (moons == null || moons.size() == 0)
			return null;
		return moons.entrySet().iterator();
	}

	public int neigbourSystemsNo() {
		if (neighborSystems != null)
			return neighborSystems.size();
		return 0;
	}

	public Iterator<Entry<Writable, Writable>> neighbourSystemsIterator() {
		if (neighborSystems != null)
			return neighborSystems.entrySet().iterator();
		return null;
	}

	// public String neighborSystemsStateToString(){
	// if(neighborSystems == null)
	// return "No neighboring system";
	// Iterator<LayeredPartitionedLongWritable> it = neighbourSystemsIterator();
	// String result = "";
	// while(it.hasNext()){
	// LayeredPartitionedLongWritable current = it.next();
	// result+= "Neighbor system: "+current.getId()+ " at layer " +
	// current.getLayer()+"\n";
	// }
	// return result;
	// }

	public LayeredPartitionedLongWritable getSun() {
		if (sun == null)
			return new LayeredPartitionedLongWritable();
		return sun;
	}

	public void setSun(LayeredPartitionedLongWritable sun) {
		this.sun = sun;
		// sunProxy = sun;
	}

	public void setSun(LayeredPartitionedLongWritable sun,
			LayeredPartitionedLongWritable favProxy) {
		this.sun = sun;
		this.favProxy = favProxy;
	}

	public LayeredPartitionedLongWritable getProxy() {
		if (!isMoon())
			return sun;
		return favProxy;
	}

	public void addToProxies(LayeredPartitionedLongWritable proxy) {
		if (sunProxies == null) {
			sunProxies = new LayeredPartitionedLongWritableSet();
		}
		sunProxies.addElement(proxy);
	}

	public LayeredPartitionedLongWritableSet getProxies() {
		return sunProxies;
	}

	// public LayeredPartitionedLongWritable getProxy(){
	// return sunProxy;
	// }

	public void clearAstralInfo() {
		cleared = true;
		planets = null;
		moons = null;
		neighborSystems = null;
		sun = null;
		sunProxies = null;
		favProxy = null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * unipg.gila.common.coordinatewritables.MiniCoordinateWritable#getWeight()
	 */
	@Override
	public int getWeight() {
		return super.getWeight() + lowerLevelWeight + astralWeight();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		cleared = in.readBoolean();
		if (cleared)
			return;
		distanceFromSun = in.readInt();
		weightFromSun = in.readInt();
		assigned = in.readBoolean();
		lowerLevelWeight = in.readInt();
		astralWeight = in.readInt();
		if (isSun()) {
			planets = new MapWritable();
			moons = new MapWritable();
			planets.readFields(in);
			moons.readFields(in);
			neighborSystems = new MapWritable();
			neighborSystems.readFields(in);
		} else {
			if (isMoon()) {
				sunProxies = new LayeredPartitionedLongWritableSet();
				sunProxies.readFields(in);
				favProxy = new LayeredPartitionedLongWritable();
				favProxy.readFields(in);
			}
			if (!isAsteroid()) {
				sun = new LayeredPartitionedLongWritable();
				sun.readFields(in);
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeBoolean(cleared);
		if (cleared)
			return;
		out.writeInt(distanceFromSun);
		out.writeInt(weightFromSun);
		out.writeBoolean(assigned);
		out.writeInt(lowerLevelWeight);
		out.writeInt(astralWeight);
		if (isSun()) {
			planets.write(out);
			moons.write(out);
			neighborSystems.write(out);
		} else {
			if (isMoon()) {
				sunProxies.write(out);
				favProxy.write(out);
			}
			if (!isAsteroid())
				sun.write(out);
		}
	}

	public Class<? extends CoordinateWritable> getTypeOfClass() {
		return this.getClass();
	}

	public static AstralBodyCoordinateWritable craftCoordinateWritable(float x,
			float y, int component) {
		return new AstralBodyCoordinateWritable(x, y, component);
	}

	/**
	 * @return
	 */
	public int planetsNo() {
		return (planets == null ? 0 : planets.size());
	}

	/**
	 * @return
	 */
	public int moonsNo() {
		return (moons == null ? 0 : moons.size());
	}

}
