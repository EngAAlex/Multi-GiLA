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
package unipg.gila.multi.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;

import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.LinkedListWritable;
import unipg.gila.common.datastructures.SetWritable;

/**
 * @author Alessio Arleo
 *
 */
public class AstralBodyCoordinateWritable extends CoordinateWritable {

	//LOGGER
	Logger log = Logger.getLogger(AstralBodyCoordinateWritable.class);
	
	protected LayeredPartitionedLongWritable favProxy;
	protected LayeredPartitionedLongWritableSet sunProxies;
	protected LayeredPartitionedLongWritable sun;
	protected int distanceFromSun=-1;
	protected MapWritable planets; //USED WHEN A BODY IS A SUN
	protected MapWritable moons; //USED WHEN A BODY IS A SUN
	protected SetWritable<LayeredPartitionedLongWritable> neighborSystems;
	protected int lowerLevelWeight = 1;
	protected boolean cleared = false;
	protected boolean assigned = false;

	public AstralBodyCoordinateWritable() {
		super();
		//		sunProxy = new LayeredPartitionedLongWritable();
//				sun = new LayeredPartitionedLongWritable();
	}


	public AstralBodyCoordinateWritable(float x, float y, JSONArray oEs,
			int component) throws JSONException {
		super(x, y, oEs, component);
		//		sunProxy = new LayeredPartitionedLongWritable();
		//		sun = new LayeredPartitionedLongWritable();
	}

	public AstralBodyCoordinateWritable(int lowerLevelWeigth, float x, float y, int component) {
		super(x, y, component);
		//		sunProxy = new LayeredPartitionedLongWritable();
		//		sun = new LayeredPartitionedLongWritable();
		this.lowerLevelWeight = lowerLevelWeigth;
	}

	public AstralBodyCoordinateWritable(float x, float y, int component) {
		super(x, y, component);
		//		sunProxy = new LayeredPartitionedLongWritable();
		//		sun = new LayeredPartitionedLongWritable();
	}

	public int astralWeight(){
		int planetsSize = 0;
		int moonsSize = 0;
		if(planets != null)
			planetsSize = planets.size();
		if(moons != null)
			moonsSize = moons.size();
		return planetsSize + moonsSize;
	}

	public int getLowerLevelWeight(){
		return lowerLevelWeight;
	}

	//	public LayeredPartitionedLongWritable getProxy(){
	//		return sunProxy;
	//	}

	public int getDistanceFromSun(){
		return distanceFromSun;
	}

	public void setAsSun(){
		distanceFromSun = 0;
	}

	public void setAsPlanet() {
		distanceFromSun = 1;
	}

	public void setAsMoon() {
		distanceFromSun = 2;
	}

	public void setAssigned(){
		assigned = true;
	}

	/**
	 * 
	 */
	public void resetAssigned() {
		assigned = false;
	}

	public boolean isAssigned(){
		return assigned;
	}

	public boolean isAsteroid(){
		return distanceFromSun == -1;
	}

	/**
	 * @return
	 */
	public boolean isPlanet() {
		return distanceFromSun == 1;
	}

	public boolean isMoon(){
		return distanceFromSun > 1;
	}

	public boolean isSun() {
		return distanceFromSun == 0;
	}

	public void resetToAsteroid(){
		distanceFromSun = -1;
	}

	public void addPlanet(LayeredPartitionedLongWritable id){
		if(planets == null)
			planets	=	new MapWritable();
		planets.put(id.copy(), new PathWritableSet());
	}

	public void addMoon(LayeredPartitionedLongWritable id){
		if(moons == null)
			moons =	new MapWritable();
		moons.put(id.copy(), new PathWritableSet());
	}

	public void addNeighbourSystem(LayeredPartitionedLongWritable sun, ReferrersList referrers, int ttl){
		if(neighborSystems == null)
			neighborSystems = new LayeredPartitionedLongWritableSet();
		log.info("Preparing to add neighbor system " + sun + " with referrers " + (referrers == null ? "zero null" : referrers.size()));
		neighborSystems.addElement(sun);
		if(referrers == null)
			return;
		Iterator<LayeredPartitionedLongWritable> it = (Iterator<LayeredPartitionedLongWritable>) referrers.iterator();
		while(it.hasNext()){
			LayeredPartitionedLongWritable currentReferrer = it.next();
			log.info("Current referrer: " + currentReferrer);
			if(planets.containsKey(currentReferrer)){
				log.info("Registering for planet " + currentReferrer + " neighbor " + sun);
				((PathWritableSet)planets.get(currentReferrer)).addElement(new PathWritable(
						1, Integer.MAX_VALUE - (ttl - 1), sun));
			}
			else {
				//########################################## WARNING!!
				//The following code contains a potential vulnerability. The bug causes the refuse message towards the refused sun to have its extra payload initialized
				//with the conflict generating vertex. The problem has no fix yet, so this patch should allow the computation to end ignoring the vertices into the referrer stacjk
				//that are neither planets nor moons.
				if(moons != null){
					PathWritableSet pSet = (PathWritableSet)moons.get(currentReferrer); 
					if(pSet != null){
						log.info("Registering for moon " + currentReferrer + " neighbor " + sun);
						pSet.addElement(new PathWritable(2, Integer.MAX_VALUE - (ttl - 1), sun));
					}
				}
			}
		}
	}

	public Iterator<Entry<Writable, Writable>> getPlanetsIterator(){
		if(planets == null || planets.size() == 0)
			return null;		
		return planets.entrySet().iterator();
	}

	public Iterator<Entry<Writable, Writable>> getMoonsIterator(){
		if(moons == null || moons.size() == 0)
			return null;
		return moons.entrySet().iterator();
	}

	public int neigbourSystemsNo(){
		if(neighborSystems != null)
			return neighborSystems.size();
		return 0;
	}

	@SuppressWarnings("unchecked")
	public Iterator<LayeredPartitionedLongWritable> neighbourSystemsIterator(){
		if(neighborSystems != null)
			return (Iterator<LayeredPartitionedLongWritable>) neighborSystems.iterator();
		return null;
	}	

	public String neighborSystemsStateToString(){
		if(neighborSystems == null)
			return "No neighboring system";
		Iterator<LayeredPartitionedLongWritable> it = neighbourSystemsIterator();
		String result = "";
		while(it.hasNext()){
			LayeredPartitionedLongWritable current = it.next();
			result+= "Neighbor system: "+current.getId()+ " at layer " + current.getLayer()+"\n";
		}
		return result;
	}

	public LayeredPartitionedLongWritable getSun() {
		if(sun == null)
			return new LayeredPartitionedLongWritable();
		return sun;
	}

	public void setSun(LayeredPartitionedLongWritable sun){
		this.sun = sun;
		//		sunProxy = sun;
	}
	
	public void setSun(LayeredPartitionedLongWritable sun, LayeredPartitionedLongWritable favProxy){
		this.sun = sun;
		this.favProxy = favProxy;
	}
	
	public LayeredPartitionedLongWritable getProxy(){
		if(isPlanet())
			return sun;
		return favProxy;
	}

	public void setProxies(LayeredPartitionedLongWritableSet proxies){
		sunProxies = proxies;
	}
	
	public void addToProxies(LayeredPartitionedLongWritable proxy){
		sunProxies.addElement(proxy);
	}
	
	public LayeredPartitionedLongWritableSet getProxies(){
		return sunProxies;
	}

	//	public LayeredPartitionedLongWritable getProxy(){
	//		return sunProxy;
	//	}

	public void clearAstralInfo(){
		cleared = true;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		cleared = in.readBoolean();
		if(cleared)
			return;
		distanceFromSun = in.readInt();
		assigned = in.readBoolean();
		lowerLevelWeight = in.readInt();
		if(isSun()){
			planets = new MapWritable();
			moons = new MapWritable();
			planets.readFields(in);
			moons.readFields(in);
			neighborSystems = new LayeredPartitionedLongWritableSet();
			neighborSystems.readFields(in);
		}else{
			if(isMoon()){
				sunProxies = new LayeredPartitionedLongWritableSet();
				sunProxies.readFields(in);
				favProxy = new LayeredPartitionedLongWritable();
				favProxy.readFields(in);	
			}
			if(!isAsteroid()){
				sun = new LayeredPartitionedLongWritable();
				sun.readFields(in);
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeBoolean(cleared);
		if(cleared)
			return;
		out.writeInt(distanceFromSun);
		out.writeBoolean(assigned);
		out.writeInt(lowerLevelWeight);		
		if(isSun()){
			planets.write(out);
			moons.write(out);
			neighborSystems.write(out);
		}else{
			if(isMoon()){
				sunProxies.write(out);
				favProxy.write(out);
			}
			if(!isAsteroid())
				sun.write(out);
		}
	}

	public Class<? extends CoordinateWritable> getTypeOfClass(){
		return this.getClass();
	}

	public static AstralBodyCoordinateWritable craftCoordinateWritable(float x, float y, int component){
		return new AstralBodyCoordinateWritable(x, y, component);
	}

}

