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
package unipg.gila.multi.coarseners;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.datastructures.SetWritable;
import unipg.gila.common.datastructures.SpTreeEdgeValue;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.common.multi.SolarMessage;
import unipg.gila.common.multi.SolarMessage.CODE;
import unipg.gila.common.multi.SolarMessageSet;
import unipg.gila.multi.MultiScaleComputation;
import unipg.gila.multi.MultiScaleMaster;

/**
 * @author Alessio Arleo
 *
 */
public class SolarMerger{

	protected final static String MERGER_MESSAGES_COUNTER = "Messages sent during merging process";

	//GLOBAL STATIC ATTRIBUTES
	public static boolean logMerger;

	/*
	 * LOGGER 
	 * */
	protected static Logger log = Logger.getLogger(SolarMerger.class);


	public static enum AstralBody{
		SUN, MOON, PLANET, ASTEROID;

		public static int valueOf(AstralBody a){
			switch(a){
			case SUN: return 0;
			case PLANET: return 1;
			case MOON: return 2;
			default: return -1;
			}
		}

		public static AstralBody buildBody(int body){
			switch(body){
			case 0: return SUN;
			case 1: return PLANET;
			case 2: return MOON;
			default: return ASTEROID;
			}
		}

		public static String toString(AstralBody a){
			switch(a){
			case SUN: return "SUN";
			case PLANET: return "PLANET";
			case MOON: return "MOON";
			default: return "ASTEROID";
			}
		}
	}

	//ALL VERTEX MUST UNDERGO THIS PROCEDURE -- NO MATTER THE LAYER
	public static class StateRestore extends AbstractComputation<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue, SolarMessage, SolarMessage> {
		
		@Override
		public void compute(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<SolarMessage> msgs) throws IOException {

			int vertexLayer = vertex.getId().getLayer();
			aggregate(SolarMergerRoutine.currentLayerAggregator, new IntWritable(vertexLayer));
						
			MapWritable infoEdges = new MapWritable();
			MapWritable infoWeights = new MapWritable();
			
			int counter = 0;
			int weightCounter = 0;
			
			Iterator<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> edges = vertex.getEdges().iterator();
			
			while(edges.hasNext()){
				Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue> e = edges.next();
				LayeredPartitionedLongWritable targetId = e.getTargetVertexId();
				if(targetId.getLayer() == vertexLayer){
					counter++;
					weightCounter = Math.max(weightCounter, e.getValue().getValue());
				}
			}
			
			infoEdges.put(new IntWritable(vertexLayer),new IntWritable(counter));
			aggregate(SolarMergerRoutine.layerEdgeSizeAggregator, infoEdges);

			infoWeights.put(new IntWritable(vertexLayer), new IntWritable(weightCounter));
			aggregate(SolarMergerRoutine.layerEdgeWeightsAggregator, infoWeights);

			if(vertex.getValue().isSun()){
				MapWritable information = new MapWritable();
				information.put(new IntWritable(vertex.getValue().getComponent()), 
						new IntWritable((int)1));
				aggregate(SolarMergerRoutine.sunsPerComponent, information);
			}
			//			//MERGER ATTEMPTS
			//			aggregate(SolarMergerRoutine.mergerAttempts, new IntWritable(((IntWritable)getAggregatedValue(SolarMergerRoutine.mergerAttempts)).get()+1));

			//LAYER VERTEX SIZE
			MapWritable infoToUpdate = new MapWritable();
			infoToUpdate.put(new IntWritable(vertexLayer), new IntWritable(1));
			aggregate(SolarMergerRoutine.layerVertexSizeAggregator, infoToUpdate);

		}		

	}

	public static class SunGeneration extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, SolarMessage>{

		protected float sunChance;

		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<SolarMessage> msgs) throws IOException {
			if(vertex.getValue().isAsteroid() && Math.random() < sunChance){
				vertex.getValue().setAsSun();
				aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
				SolarMessage smg = new SolarMessage(vertex.getId(), 1, vertex.getId(), CODE.SUNOFFER);
				smg.setWeight(vertex.getValue().astralWeight());
				sendMessageToAllEdges(vertex, smg);
			}
		}

		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);

			sunChance = ((FloatWritable)getAggregatedValue(SolarMergerRoutine.sunChanceAggregatorString)).get();
		}

	}

	public static class SolarSweep extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, SolarMessage>{

		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<SolarMessage> msgs) throws IOException {

			if(vertex.getValue().isAssigned())
				return;

			int astralWeight = vertex.getValue().astralWeight();
			long vertexID = vertex.getId().getId();
			SolarMessage chosenOne = null;			


			Iterator<SolarMessage> theMessages = msgs.iterator();
			long maxID = Long.MIN_VALUE;
			int minWeight = Integer.MAX_VALUE;
			//			SolarMessage chosenOneForWeight = null;
			//			SolarMessage chosenOneForId = null;			
			while(theMessages.hasNext()){
				SolarMessage current = theMessages.next();
				long currentID = current.getPayloadVertex().getId();
				if(currentID == vertexID)
					continue;

				int currentWeight = current.getWeight();

				if(currentWeight < minWeight){
					minWeight = current.getWeight();
					chosenOne = current.copy();
					maxID = currentID;
				}else if(current.getPayloadVertex().getId() > maxID){
					maxID = currentID;
					chosenOne = current.copy();
				}

			}

			if(chosenOne == null)
				return;

			if(vertex.getValue().isSun()){ 
				if(astralWeight > minWeight || (astralWeight == minWeight && maxID > vertexID)){
					vertex.getValue().resetToAsteroid();
				}
			}


			//			if(chosenOneForWeight != null && chosenOneForId != null){
			//				SolarMessage finalMessage = chosenOneForWeight == null ? chosenOneForId : chosenOneForWeight;
			if(chosenOne != null && !chosenOne.isAZombie()){
				sendMessageToAllEdges(vertex, (SolarMessage) chosenOne.propagate());
				aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
			}
			//			}
		}
	}

	public static class SunBroadcast extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, SolarMessage>{

		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<SolarMessage> msgs) throws IOException{
			if(vertex.getValue().isSun() && !vertex.getValue().isAssigned()){
				SolarMessage sunBroadcast = new SolarMessage(vertex.getId(), 1, vertex.getId(), CODE.SUNOFFER);
				sunBroadcast.setWeight(0);
				sendMessageToAllEdgesWithWeight(vertex, sunBroadcast);
				if(logMerger)
					log.info("I'm " + vertex.getId().getId()+ " and I'm broadcasting my sun offer");
			}
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.MultiScaleComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
		 */
		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);
		}

	}



	public static class PlanetResponse extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, SolarMessage>{

		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);
		}

		@SuppressWarnings({ "unchecked" })
		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<SolarMessage> msgs) throws IOException{

			if(!vertex.getValue().isAsteroid() || vertex.getValue().isAssigned())
				return;

			Iterator<SolarMessage> theMessages = (Iterator<SolarMessage>)msgs.iterator();

			theMessages = (Iterator<SolarMessage>)msgs.iterator();
			AstralBodyCoordinateWritable value = vertex.getValue();

			//THE MESSAGES ARE NOW SHUFFLED TO CHOOSE THE NEW SUN.
			Writable[] shuffled = maxIdShufflerWithWeights(vertex.getId().getId(), theMessages, value);

			SolarMessage chosenOne = (SolarMessage) shuffled[0];	
			SetWritable<SolarMessage> refused = (SetWritable<SolarMessage>) shuffled[1];

			if(chosenOne != null){
				if(logMerger)
					log.info("Chosen message " + chosenOne);
				value.setSun(chosenOne.getValue().copy(), chosenOne.getPayloadVertex().copy());
				//SET THE SUN
				if(chosenOne.getTTL() == 1){
					value.setAsPlanet(chosenOne.getWeight());
					if(logMerger)
						log.info("Me, vertex " + vertex.getId().getId() + " becoming a planet of sun " + chosenOne.getValue().getId() + " weight " + chosenOne.getWeight());
				}else{
					if(logMerger){
						log.info("Me, vertex " + vertex.getId().getId() + " becoming a moon of sun " + chosenOne.getValue().getId() + " weight " + chosenOne.getWeight());
						log.info("I'm a moon and adding to proxy " + chosenOne.getPayloadVertex());
					}
					value.setAsMoon(chosenOne.getWeight());
					value.addToProxies(chosenOne.getPayloadVertex().copy());
				}
				
				ackAndPropagateSunOffer(vertex, value, chosenOne);
				
			}

			if(refused != null){
				Iterator<SolarMessage> offersToRefuse;
				if(value.isMoon()){
					offersToRefuse =  (Iterator<SolarMessage>) refused.iterator();
					while(offersToRefuse.hasNext()){ //build proxies
						SolarMessage current = offersToRefuse.next();
						if(current.getValue().equals(value.getSun())){
							if(logMerger)
								log.info("I'm a moon and adding to proxy " + current.getPayloadVertex());
							value.addToProxies(current.getPayloadVertex().copy());
						}
					}	
				}
				offersToRefuse =  (Iterator<SolarMessage>) refused.iterator();
				while(offersToRefuse.hasNext())
					refuseOffer(vertex, offersToRefuse.next());
			}
		}

		protected Writable[] maxIdShufflerWithWeights(Long vertexId, Iterator<SolarMessage> theMessages, AstralBodyCoordinateWritable value){
			SolarMessage chosenOne = null;
			SetWritable<SolarMessage> refusedOffers = null;
			//		SetWritable<LayeredPartitionedLongWritable> incomingInterfaces = null;
			while(theMessages.hasNext()){
				SolarMessage current = theMessages.next();
				if(logMerger)
					log.info("Received " + current);
				if(!current.getCode().equals(CODE.SUNOFFER)) // || sunsToIgnore.contains(current.getPayloadVertex().getId()))
					continue;
				if(chosenOne == null || 
						(/*current.getPayloadVertex().getId() > vertexId && */
								current.getWeight() < chosenOne.getWeight()// ||
								//								(current.getWeight() == chosenOne.getWeight() && current.getPayloadVertex().getId() > chosenOne.getPayloadVertex().getId())	//)
								&&  !current.getValue().equals(chosenOne.getValue()))){	
					if(chosenOne != null){
						if(refusedOffers == null)
							refusedOffers = new SolarMessageSet();
						refusedOffers.addElement(chosenOne.copy());
					}
					chosenOne = current.copy();
				}else{
					if(refusedOffers == null)
						refusedOffers = new SolarMessageSet();
					refusedOffers.addElement(current.copy());
				}
			}
			return new Writable[]{chosenOne, refusedOffers};//, incomingInterfaces};
		}

		protected Writable[] maxIdShuffler(Long vertexId, Iterator<SolarMessage> theMessages, AstralBodyCoordinateWritable value){
			SolarMessage chosenOne = null;
			SetWritable<SolarMessage> refusedOffers = null;
			//		SetWritable<LayeredPartitionedLongWritable> incomingInterfaces = null;
			while(theMessages.hasNext()){
				SolarMessage current = theMessages.next();
				if(logMerger)
					log.info("Received " + current);
				if(!current.getCode().equals(CODE.SUNOFFER)) // || sunsToIgnore.contains(current.getPayloadVertex().getId()))
					continue;
				if(chosenOne == null || 
						(current.getPayloadVertex().getId() > vertexId && current.getPayloadVertex().getId() > chosenOne.getPayloadVertex().getId())
						&&  !current.getValue().equals(chosenOne.getValue())){	
					if(chosenOne != null){
						if(refusedOffers == null)
							refusedOffers = new SolarMessageSet();
						refusedOffers.addElement(chosenOne.copy());
					}
					chosenOne = current.copy();
				}else{
					if(refusedOffers == null)
						refusedOffers = new SolarMessageSet();
					refusedOffers.addElement(current.copy());
				}
			}
			return new Writable[]{chosenOne, refusedOffers};//, incomingInterfaces};
		}

		@SuppressWarnings("unchecked")
		protected void refuseOffer(Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex, SolarMessage refusedSun){
			//INFORM MY SUN THAT AN OFFER HAS BEEN REFUSED
			SolarMessage smForMySun = new SolarMessage(vertex.getId(), Integer.MAX_VALUE - (refusedSun.getTTL() == 0 ? 2 : 1), refusedSun.getValue().copy(), CODE.REFUSEOFFER);
			smForMySun.addToExtraPayload(vertex.getId(), refusedSun.getWeight());
			smForMySun.setWeight(refusedSun.getWeight());
			if(vertex.getValue().isPlanet()){
				sendMessageWithWeight(vertex, vertex.getValue().getSun(), smForMySun);
			}else
				sendMessageToMultipleEdgesWithWeight(vertex, (Iterator<LayeredPartitionedLongWritable>) vertex.getValue().getProxies().iterator(), smForMySun);

			//INFORM THE REFUSED SUN THAT ITS OFFER HAS BEEN DECLINED
			SolarMessage declinedMessage = new SolarMessage(vertex.getId(), Integer.MAX_VALUE - vertex.getValue().getDistanceFromSun(), vertex.getValue().getSun().copy(), CODE.REFUSEOFFER);
			declinedMessage.setWeight(vertex.getValue().getWeightFromSun());
			sendMessageWithWeight(vertex, refusedSun.getPayloadVertex(), declinedMessage);
			aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
		}

		protected void ackAndPropagateSunOffer(Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex, AstralBodyCoordinateWritable value,
				SolarMessage chosenOne){
			//ACK THE SUN OFFER
			SolarMessage smg = new SolarMessage(vertex.getId(), 1, chosenOne.getValue().copy(), CODE.ACCEPTOFFER);
			smg.setSolarWeight(vertex.getValue().astralWeight());
			sendMessage(value.getProxy(), smg);

			//IF NEEDED PROPAGATE THE SOLAR MESSAGE
			if(chosenOne.getCode().equals(CODE.SUNOFFER) && !chosenOne.isAZombie()){
				chosenOne.spoofPayloadVertex(vertex.getId());
				SolarMessage sls = (SolarMessage) chosenOne.propagate();
				sendMessageToAllEdgesWithWeight(vertex, sls);
			}

			aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
		}
	}

	public static class RegimeMerger extends PlanetResponse{
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<SolarMessage> msgs) throws IOException{
			AstralBodyCoordinateWritable value = vertex.getValue();
			Iterator<SolarMessage> msgIterator;
			AstralBody status = AstralBody.buildBody(value.getDistanceFromSun());
			if(logMerger)
				log.info("I'm " + vertex.getId());
			switch(status){

			case SUN: //#### A SUN EXAMINES THE MESSAGES; PLANET/MOON MESSAGES ARE PROCESSED AND STORED. FIRST ALL ACCEPTED OFFERS ARE COMPUTED, THEN THE 
				msgIterator = msgs.iterator(); //REFUSED ONES. 
				while(msgIterator.hasNext()){
					SolarMessage currentMessage =  msgIterator.next();

					if(currentMessage.getPayloadVertex() == vertex.getId()) //DISCARD MESSAGES GENERATED BY ME (VERTEX)
						return;

					if(currentMessage.getCode().equals(CODE.ACCEPTOFFER)) //A PLANET/MOON HAS ACCEPTED THE OFFER; IT IS STORED INTO THE APPROPRIATE DATA STRUCTURE
						if(currentMessage.getTTL() == 1){
							if(logMerger){
								log.info("Registering planet " + currentMessage.getPayloadVertex() + " solar weight " + currentMessage.getSolarWeight());
							}
							value.addPlanet(currentMessage.getPayloadVertex().copy(), currentMessage.getSolarWeight());
						}else{
							if(logMerger){
								log.info("Registering moon " + currentMessage.getPayloadVertex() + " solar weight " + currentMessage.getSolarWeight());
							}
							value.addMoon(currentMessage.getPayloadVertex().copy(), currentMessage.getSolarWeight());
						}
				}
				msgIterator = msgs.iterator();
				while(msgIterator.hasNext()){
					SolarMessage currentMessage =  msgIterator.next();					
					if(!currentMessage.getValue().equals(vertex.getId()) && (currentMessage.getCode().equals(CODE.REFUSEOFFER) || currentMessage.getCode().equals(CODE.SUNDISCOVERY))){ //THE SUN OFFER HAS BEEN DECLINED. SAVING THE DATA INTO THE NEIGHBORING SYSTEMS DATA STR.
						if(logMerger){
							log.info("Registering for referenced sun " + currentMessage.getValue() + " total weight " + currentMessage.getWeight() + " " +
									currentMessage.getExtraPayload().toString());
						}
						value.addNeighbourSystem(currentMessage.getValue(), currentMessage.getExtraPayload(), currentMessage.getWeight());
					}
				}
				// SUNS SHALL NOT REACT TO ANY OTHER MESSAGE TYPE.
				break;

			case ASTEROID: //####AN ASTEROID RECEIVES MESSAGES: THE CHOICE OF THE SUN IS MADE LIKE IN THE PLANET RESPONSE STEP

				super.vertexInLayerComputation(vertex, msgs);
				break;

			default: //####A PLANET OR A MOON RECEIVES MESSAGES. REFUSE OFFERS ARE PROPAGATED THROUGH PROXIES
				msgIterator = msgs.iterator();					
				while(msgIterator.hasNext()){
					SolarMessage currentMessage =  msgIterator.next();
					if(logMerger)
						log.info("Received " + currentMessage);
					if(currentMessage.getCode().equals(CODE.ACCEPTOFFER) && currentMessage.getValue().equals(value.getSun())){
						sendMessage(value.getProxy(), (SolarMessage)currentMessage.propagate());
						aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
					}

					if(currentMessage.getCode().equals(CODE.REFUSEOFFER)){// && !vertex.getValue().isAssigned()) || currentMessage.getCode().equals(CODE.SUNDISCOVERY)){
						SolarMessage messageToSend = (SolarMessage)currentMessage.propagate();
						messageToSend.addToExtraPayload(vertex.getId(), messageToSend.getWeight());
						if(vertex.getValue().isPlanet()){
							if(logMerger)
								log.info("Propagating " + currentMessage + " with updated weight " + vertex.getEdgeValue(vertex.getValue().getSun()).getValue());
							sendMessageWithWeight(vertex, vertex.getValue().getSun(), messageToSend);
						}else{
							sendMessageToMultipleEdgesWithWeight(vertex, (Iterator<LayeredPartitionedLongWritable>) vertex.getValue().getProxies().iterator(), messageToSend);
						}
						aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
					}
				}
				break;
			}

		}

	}

	/**
	 * This computation is used to complete the merging round, forcing the frontier moons to discover neighboring systems. 
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class MoonSweep extends MultiScaleComputation<AstralBodyCoordinateWritable,SolarMessage, SolarMessage>{

		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<SolarMessage> msgs) throws IOException {
			AstralBodyCoordinateWritable value = vertex.getValue();
			if(!value.isSun()){
				aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
				SolarMessage sweepMessage = new SolarMessage(vertex.getId(), Integer.MAX_VALUE - vertex.getValue().getDistanceFromSun(), value.getSun(), CODE.SUNDISCOVERY);
				sweepMessage.setWeight(value.getWeightFromSun());
				sendMessageToAllEdgesWithWeight(vertex, sweepMessage);
			}
		}
	}

	/**
	 * This computation receives the messages from the MoonSweep computation and creates fake refuse messages 
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class SunDiscovery extends MultiScaleComputation<AstralBodyCoordinateWritable,SolarMessage, SolarMessage>{

		@SuppressWarnings("unchecked")
		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<SolarMessage> msgs) {
			Iterator<SolarMessage> messages = msgs.iterator();
			AstralBodyCoordinateWritable value = vertex.getValue();
			if(logMerger)
				log.info("Sun Discovery");
			while(messages.hasNext()){
				SolarMessage xu = messages.next();
				if(logMerger)
					log.info(xu);
				if(xu.getCode().equals(CODE.SUNDISCOVERY)){
					if((value.isSun() && xu.getValue().equals(vertex.getId()) || xu.getValue().equals(value.getSun())))
						continue;
					aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
					SolarMessage messageForReferrer = new SolarMessage(xu.getPayloadVertex().copy(), Integer.MAX_VALUE - value.getDistanceFromSun(), (value.isSun() ? vertex.getId() : value.getSun()), CODE.REFUSEOFFER);
					messageForReferrer.setWeight(value.getWeightFromSun());
					if(logMerger)
						log.info("Informing the referrer about me " + messageForReferrer);
					sendMessageWithWeight(vertex, xu.getPayloadVertex().copy(), messageForReferrer);
					if(!value.isSun()){
						SolarMessage messageForSun = new SolarMessage(vertex.getId(), xu.getTTL() - 1, xu.getValue().copy(), CODE.REFUSEOFFER);
						messageForSun.setWeight(xu.getWeight());
						messageForSun.addToExtraPayload(vertex.getId(), xu.getWeight());
						if(logMerger)
							log.info("informing my sun " + messageForSun);
						if(vertex.getValue().isPlanet())
							sendMessageWithWeight(vertex, vertex.getValue().getSun(), messageForSun);
						else{
							sendMessageToMultipleEdgesWithWeight(vertex, (Iterator<LayeredPartitionedLongWritable>) vertex.getValue().getProxies().iterator(), messageForSun);
						}
					}else //ISOLATED SUN
						value.addNeighbourSystem(xu.getValue().copy(), null, xu.getWeight());
				}
			}
		}

	}		

	public static class AsteroidCaller extends MultiScaleComputation<AstralBodyCoordinateWritable,SolarMessage, SolarMessage>{

		/* (non-Javadoc)
		 * @see unipg.dafne.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
		 */
		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<SolarMessage> msgs) throws IOException {
			if(vertex.getValue().isAsteroid()){
				aggregate(SolarMergerRoutine.asteroidsRemoved, new BooleanWritable(false));
			}else
				vertex.getValue().setAssigned();
		}

	}

	public static class SolarMergeVertexCreation extends MultiScaleComputation<AstralBodyCoordinateWritable,SolarMessage, SolarMessage>{

		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<SolarMessage> msgs) throws IOException{
			if(!vertex.getValue().isSun())
				return;

			if(logMerger)
				log.info("I'm " + vertex.getId());

			//####REGISTERING DATA TO AGGREGATORS
			//SUNS PER COMPONENT
			MapWritable information = new MapWritable();
			information.put(new IntWritable(vertex.getValue().getComponent()), 
					new IntWritable((int)1));
			aggregate(SolarMergerRoutine.sunsPerComponent, information);
			//MERGER ATTEMPTS
			aggregate(SolarMergerRoutine.mergerAttempts, new IntWritable(((IntWritable)getAggregatedValue(SolarMergerRoutine.mergerAttempts)).get()+1));
			//LAYER VERTEX SIZE
			MapWritable infoToUpdate = new MapWritable();
			infoToUpdate.put(new IntWritable(currentLayer+1), new IntWritable(1));
			aggregate(SolarMergerRoutine.layerVertexSizeAggregator, infoToUpdate);

			AstralBodyCoordinateWritable value = vertex.getValue();
			double[] coords = value.getCoordinates();

			LayeredPartitionedLongWritable homologousId = new LayeredPartitionedLongWritable(vertex.getId().getPartition(), 
					vertex.getId().getId(), 
					vertex.getId().getLayer()+1);

			addEdgeRequest(vertex.getId(), EdgeFactory.create(homologousId, new SpTreeEdgeValue(1)));					

			ByteArrayEdges<LayeredPartitionedLongWritable, SpTreeEdgeValue> outEdges = new ByteArrayEdges<LayeredPartitionedLongWritable, SpTreeEdgeValue>();
			outEdges.setConf(getSpecialConf());

			List<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> edgeList = new LinkedList<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>>();
			edgeList.add(EdgeFactory.create(vertex.getId(), new SpTreeEdgeValue(1)));
			int counter = 0;
			int weightCounter = 0;

			if(vertex.getValue().neigbourSystemsNo() > 0){
				Iterator<Entry<Writable, Writable>> neighborSystems = vertex.getValue().neighbourSystemsIterator();
				while(neighborSystems.hasNext()){
					Entry<Writable, Writable> current = neighborSystems.next();
					LayeredPartitionedLongWritable neighborSun = (LayeredPartitionedLongWritable) current.getKey();
					if(logMerger)
						log.info("connecting vertex " + neighborSun);
					edgeList.add(EdgeFactory.create(new LayeredPartitionedLongWritable(neighborSun.getPartition(), neighborSun.getId(), neighborSun.getLayer() + 1),
							new SpTreeEdgeValue(((IntWritable)current.getValue()).get())));
					weightCounter = Math.max(weightCounter, ((IntWritable)current.getValue()).get());
					counter++;
				}
			}

			outEdges.initialize(edgeList);
			if(logMerger)
				log.info("Creating a new vertx with lowerweight " + value.astralWeight());
			addVertexRequest(homologousId, new AstralBodyCoordinateWritable(value.astralWeight(), 
					coords[0], coords[1],value.getComponent()), outEdges);
			MapWritable infoEdges = new MapWritable();
			MapWritable infoWeights = new MapWritable();

			infoWeights.put(new IntWritable(currentLayer+1), new IntWritable(weightCounter));
			aggregate(SolarMergerRoutine.layerEdgeWeightsAggregator, infoWeights);

			infoEdges.put(new IntWritable(currentLayer+1),new IntWritable(counter));
			aggregate(SolarMergerRoutine.layerEdgeSizeAggregator, infoEdges);
		}

	}

	public static class DummySolarMergerComputation extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, SolarMessage>{

		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
				Iterable<SolarMessage> msgs) throws IOException {
			vertex.getValue().resetAssigned();
		}
	}

}



