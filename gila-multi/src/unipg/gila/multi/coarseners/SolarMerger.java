package unipg.gila.multi.coarseners;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.OutEdges;
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

import unipg.gila.common.datastructures.SetWritable;
import unipg.gila.multi.MultiScaleComputation;
import unipg.gila.multi.common.AstralBodyCoordinateWritable;
import unipg.gila.multi.common.LayeredPartitionedLongWritable;
import unipg.gila.multi.common.SolarMessage;
import unipg.gila.multi.common.SolarMessage.CODE;
import unipg.gila.multi.common.SolarMessageSet;

/**
 * @author Alessio Arleo
 *
 */
public class SolarMerger{

	/*
	 * LOGGER 
	 * */
//	protected static Logger log = Logger.getLogger(SolarMerger.class);

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

	public static class SunGeneration extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, SolarMessage>{

		protected float sunChance;

		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
				Iterable<SolarMessage> msgs) throws IOException {
			if(vertex.getValue().isAsteroid() && Math.random() < sunChance){
				vertex.getValue().setAsSun();
				MapWritable myValue = new MapWritable();
				myValue.put(new IntWritable(currentLayer), new IntWritable(1));
				aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
				sendMessageToAllEdges(vertex, new SolarMessage(vertex.getId(), 1, vertex.getId(), CODE.SUNOFFER));
			}
		}

		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);

			sunChance = ((FloatWritable)getAggregatedValue(SolarMergerRoutine.sunChanceAggregatorString)).get();
		}

	}

	public static class SunBroadcast extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, SolarMessage>{


		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
				Iterable<SolarMessage> msgs) throws IOException{
			if(vertex.getValue().isSun() && !vertex.getValue().isAssigned()){
				sendMessageToAllEdges(vertex, new SolarMessage(vertex.getId(), 1, vertex.getId(), CODE.SUNOFFER));
//				log.info("I'm " + vertex.getId().getId()+ " and I'm broadcasting my sun offer");
			}
		}


	}

	public static class SolarSweep extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, SolarMessage>{

		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
				Iterable<SolarMessage> msgs) throws IOException {

			if(vertex.getValue().isAssigned())
				return;

			Iterator<SolarMessage> theMessages = msgs.iterator();
			long maxID = Integer.MIN_VALUE;
			SolarMessage chosenOne = null;
			while(theMessages.hasNext()){
				SolarMessage current = theMessages.next();
				if(current.getPayloadVertex().getId() > maxID && current.getPayloadVertex().getId() != vertex.getId().getId()){
					maxID = current.getPayloadVertex().getId();
					chosenOne = current.copy();
				}
			}

			if(chosenOne == null)
				return;

			if(vertex.getValue().isSun() && maxID > vertex.getId().getId()){
				vertex.getValue().resetToAsteroid();
			}

			if(!chosenOne.isAZombie()){
				sendMessageToAllEdges(vertex, (SolarMessage) chosenOne.propagate());
				aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
			}
		}


	}

	public static class PlanetResponse extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, SolarMessage>{

		@Override
		public void initialize(
				GraphState graphState,
				WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> workerClientRequestProcessor,
				GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> graphTaskManager,
				WorkerGlobalCommUsage workerGlobalCommUsage,
				WorkerContext workerContext) {
			super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
					workerGlobalCommUsage, workerContext);
		}

		@SuppressWarnings({ "unchecked" })
		@Override
		protected void vertexInLayerComputation(
				Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
				Iterable<SolarMessage> msgs) throws IOException{

			if(!vertex.getValue().isAsteroid() || vertex.getValue().isAssigned())
				return;

			Iterator<SolarMessage> theMessages = (Iterator<SolarMessage>)msgs.iterator();

			theMessages = (Iterator<SolarMessage>)msgs.iterator();
			AstralBodyCoordinateWritable value = vertex.getValue();

			//THE MESSAGES ARE NOW SHUFFLED TO CHOOSE THE NEW SUN.
			Writable[] shuffled = maxIdShuffler(vertex.getId().getId(), theMessages, value);

			SolarMessage chosenOne = (SolarMessage) shuffled[0];
			SetWritable<SolarMessage> refused = (SetWritable<SolarMessage>) shuffled[1];

			if(chosenOne != null){
				value.setSun(chosenOne.getPayloadVertex().copy(), chosenOne.getValue().copy());
				ackAndPropagateSunOffer(vertex, value, chosenOne);
				//SET THE SUN
				if(chosenOne.getTTL() == 1){
					value.setAsPlanet();
					//					log.info("Me, vertex " + vertex.getId().getId() + " becoming a planet of sun " + chosenOne.getValue().getId());
				}else{
					//					log.info("Me, vertex " + vertex.getId().getId() + " becoming a moon of sun " + chosenOne.getValue().getId());

					value.setAsMoon();
				}
			}

			if(refused != null){
				Iterator<SolarMessage> offersToRefuse =  (Iterator<SolarMessage>) refused.iterator();
				while(offersToRefuse.hasNext())
					refuseOffer(vertex, offersToRefuse.next());
			}
		}

		protected Writable[] maxIdShuffler(Long vertexId, Iterator<SolarMessage> theMessages, AstralBodyCoordinateWritable value){
			SolarMessage chosenOne = null;
			SetWritable<SolarMessage> refusedOffers = null;
			//		SetWritable<LayeredPartitionedLongWritable> incomingInterfaces = null;
			while(theMessages.hasNext()){
				SolarMessage current = theMessages.next();
				if(!current.getCode().equals(CODE.SUNOFFER) || (chosenOne != null && current.getValue().equals(chosenOne.getValue()))) // || sunsToIgnore.contains(current.getPayloadVertex().getId()))
					continue;
				if(chosenOne == null || (current.getPayloadVertex().getId() > vertexId && current.getPayloadVertex().getId() > chosenOne.getPayloadVertex().getId())){	
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

		protected void refuseOffer(Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex, SolarMessage refusedSun){
			//INFORM MY SUN THAT AN OFFER HAS BEEN REFUSED
			SolarMessage smForMySun = new SolarMessage(vertex.getId(), Integer.MAX_VALUE - (refusedSun.getTTL() == 0 ? 2 : 1), refusedSun.getValue().copy(), CODE.REFUSEOFFER);
			smForMySun.addToExtraPayload(vertex.getId());
			sendMessage(vertex.getValue().getProxy(), smForMySun);

			//INFORM THE REFUSED SUN THAT ITS OFFER HAS BEEN DECLINED
			SolarMessage declinedMessage = new SolarMessage(vertex.getId(), Integer.MAX_VALUE - vertex.getValue().getDistanceFromSun(), vertex.getValue().getSun().copy(), CODE.REFUSEOFFER);
			//			log.info("Me, vertex " + vertex.getId().getId()  + "Refusing offer from " + refusedSun.getValue().getId() + " received from " + refusedSun.getPayloadVertex() + " sending thru " + refusedSun.getPayloadVertex());
//			if(declinedMessage.getExtraPayload() != null)
//				log.info("The declinedMessage contains extra payload " + declinedMessage.getExtraPayload().toString());
//			else
//				log.info("The declined message contains no extra payload");
			sendMessage(refusedSun.getPayloadVertex(), declinedMessage);
			aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
		}

		protected void ackAndPropagateSunOffer(Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex, AstralBodyCoordinateWritable value,
				SolarMessage chosenOne){
			//ACK THE SUN OFFER
			sendMessage(value.getProxy(), new SolarMessage(vertex.getId(), 1, chosenOne.getValue().copy(), CODE.ACCEPTOFFER));

			//IF NEEDED PROPAGATE THE SOLAR MESSAGE
			if(chosenOne.getCode().equals(CODE.SUNOFFER) && !chosenOne.isAZombie()){
				chosenOne.spoofPayloadVertex(vertex.getId());
				sendMessageToAllEdges(vertex, (SolarMessage) chosenOne.propagate());
			}

			aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
		}

		public static class RegimeMerger extends PlanetResponse{
			protected void vertexInLayerComputation(
					Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
					Iterable<SolarMessage> msgs) throws IOException{
				AstralBodyCoordinateWritable value = vertex.getValue();
				Iterator<SolarMessage> msgIterator;
				AstralBody status = AstralBody.buildBody(value.getDistanceFromSun());
				switch(status){

				case SUN: //#### A SUN EXAMINES THE MESSAGES; PLANET/MOON MESSAGES ARE PROCESSED AND STORED. FIRST ALL ACCEPTED OFFERS ARE COMPUTED, THEN THE 
					msgIterator = msgs.iterator(); //REFUSED ONES. 
					while(msgIterator.hasNext()){
						SolarMessage currentMessage =  msgIterator.next();

						if(currentMessage.getPayloadVertex() == vertex.getId()) //DISCARD MESSAGES GENERATED BY ME (VERTEX)
							return;

						if(currentMessage.getCode().equals(CODE.ACCEPTOFFER)) //A PLANET/MOON HAS ACCEPTED THE OFFER; IT IS STORED INTO THE APPROPRIATE DATA STRUCTURE
							if(currentMessage.getTTL() == 1){
//								log.info("Me, sun " + vertex.getId().getId() + " accept as a planet the vertex " + currentMessage.getPayloadVertex());
								value.addPlanet(currentMessage.getPayloadVertex().copy());
							}else{
//								log.info("Me, sun " + vertex.getId().getId() + " accept as a moon the vertex " + currentMessage.getPayloadVertex());
								value.addMoon(currentMessage.getPayloadVertex().copy());
							}
					}
					msgIterator = msgs.iterator();
					while(msgIterator.hasNext()){
						SolarMessage currentMessage =  msgIterator.next();					
						if(currentMessage.getCode().equals(CODE.REFUSEOFFER) || currentMessage.getCode().equals(CODE.SUNDISCOVERY)){ //THE SUN OFFER HAS BEEN DECLINED. SAVING THE DATA INTO THE NEIGHBORING SYSTEMS DATA STR.
//							log.info("Me, sun " + vertex.getId().getId() + " accept as a neighboring sun the vertex " + currentMessage.getValue());
//							if(currentMessage.getExtraPayload() != null)
//								log.info("Referrers " + currentMessage.getExtraPayload().toString());
//							else
//								log.info("No referrers in this message");
							value.addNeighbourSystem(currentMessage.getValue(), currentMessage.getExtraPayload(), currentMessage.getTTL());
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
						//						SolarMessage messageToSend;
						//						if(!currentMessage.getCode().equals(CODE.REFUSEOFFER)){
						//							SolarMessage messageToSend = (SolarMessage)currentMessage.propagate();
						//							sendMessage(value.getProxy(), messageToSend);
						//							aggregate(MultiScaleDirector.messagesDepleted, new BooleanWritable(false));
						//						}else{
						if((currentMessage.getCode().equals(CODE.REFUSEOFFER) && !vertex.getValue().isAssigned()) || currentMessage.getCode().equals(CODE.SUNDISCOVERY)){
//							log.info("Me, vertex " + vertex.getId() + "Refusemessage from sun " + currentMessage.getValue() + " thru " + currentMessage.getPayloadVertex() +
//									"I am adding my info to it with current info ");
//							if(currentMessage.getExtraPayload() != null)
//								log.info(currentMessage.getExtraPayload().toString());
//							else
//								log.info("Message contains no extra Payload");
							SolarMessage messageToSend = (SolarMessage)currentMessage.propagate();
							messageToSend.addToExtraPayload(vertex.getId());
							sendMessage(value.getProxy(), messageToSend);
							aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
						}
						continue;
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
					Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
					Iterable<SolarMessage> msgs) throws IOException {
				AstralBodyCoordinateWritable value = vertex.getValue();
				if(value.isMoon()){
					aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
					SolarMessage sweepMessage = new SolarMessage(vertex.getId(), Integer.MAX_VALUE - 2, value.getSun(), CODE.SUNDISCOVERY);
					sendMessageToAllEdges(vertex, sweepMessage);
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

			@Override
			protected void vertexInLayerComputation(
					Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
					Iterable<SolarMessage> msgs) {
				Iterator<SolarMessage> messages = msgs.iterator();
				AstralBodyCoordinateWritable value = vertex.getValue();
				while(messages.hasNext()){
					SolarMessage xu = messages.next();
					if(xu.getCode().equals(CODE.SUNDISCOVERY) && !xu.getValue().equals(value.getSun())){
						aggregate(SolarMergerRoutine.messagesDepleted, new BooleanWritable(false));
						SolarMessage messageForReferrer = new SolarMessage(xu.getPayloadVertex().copy(), Integer.MAX_VALUE - value.getDistanceFromSun(), value.isSun() ? vertex.getId() : value.getSun(), CODE.SUNDISCOVERY);
						sendMessage(xu.getPayloadVertex().copy(), messageForReferrer);
						if(!value.isSun()){
							SolarMessage messageForSun = new SolarMessage(vertex.getId(), xu.getTTL() - 1, xu.getValue().copy(), CODE.SUNDISCOVERY);
							messageForSun.addToExtraPayload(vertex.getId());
							sendMessage(value.getProxy(), messageForSun);
						}else
							value.addNeighbourSystem(xu.getValue().copy(), null, xu.getTTL() - 1);
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
					Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
					Iterable<SolarMessage> msgs) throws IOException {
				if(vertex.getValue().isAsteroid())
					aggregate(SolarMergerRoutine.asteroidsRemoved, new BooleanWritable(false));
				else
					vertex.getValue().setAssigned();
			}

		}

		public static class SolarMergeVertexCreation extends MultiScaleComputation<AstralBodyCoordinateWritable,SolarMessage, SolarMessage>{

			@Override
			protected void vertexInLayerComputation(
					Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
					Iterable<SolarMessage> msgs) throws IOException{
				if(!vertex.getValue().isSun())
					return;
				
				aggregate(SolarMergerRoutine.mergerAttempts, new IntWritable(((IntWritable)getAggregatedValue(SolarMergerRoutine.mergerAttempts)).get()+1));
				MapWritable infoToUpdate = new MapWritable();
				infoToUpdate.put(new IntWritable(currentLayer+1), new IntWritable(1));
				aggregate(SolarMergerRoutine.layerSizeAggregator, infoToUpdate);
				
				AstralBodyCoordinateWritable value = vertex.getValue();
				float[] coords = value.getCoordinates();
			
				LayeredPartitionedLongWritable homologousId = new LayeredPartitionedLongWritable(vertex.getId().getPartition(), 
						vertex.getId().getId(), 
						vertex.getId().getLayer()+1);
				
			
				addEdgeRequest(vertex.getId(), EdgeFactory.create(homologousId, new FloatWritable(1.0f)));					

				Iterator<LayeredPartitionedLongWritable> neighborSystems = vertex.getValue().neighbourSystemsIterator();
//				log.info("Vertex " + vertex.getId() + " is creating " + homologousId);
				
				if(neighborSystems == null){
					addVertexRequest(homologousId, new AstralBodyCoordinateWritable(value.astralWeight(), 
							coords[0], coords[1],value.getComponent()));					
					return;
				}

//				log.info("Connecting edges on the upper level, vertex " + vertex.getId() + " its homologous " + homologousId);

				ByteArrayEdges<LayeredPartitionedLongWritable, FloatWritable> outEdges = new ByteArrayEdges<LayeredPartitionedLongWritable, FloatWritable>();
				outEdges.setConf(getSpecialConf());
				
				List<Edge<LayeredPartitionedLongWritable, FloatWritable>> edgeList = new LinkedList<Edge<LayeredPartitionedLongWritable, FloatWritable>>();
				edgeList.add(EdgeFactory.create(vertex.getId(), new FloatWritable(1.0f)));
				
				while(neighborSystems.hasNext()){
					LayeredPartitionedLongWritable neighborSun = neighborSystems.next();
					edgeList.add(EdgeFactory.create(new LayeredPartitionedLongWritable(neighborSun.getPartition(), neighborSun.getId(), neighborSun.getLayer() + 1), new FloatWritable(1.0f)));
				}
				
				outEdges.initialize(edgeList);
				addVertexRequest(homologousId, new AstralBodyCoordinateWritable(value.astralWeight(), 
								coords[0], coords[1],value.getComponent()), outEdges);
			}

		}

		//		public static class EdgeDuplicatesRemover extends MultiScaleComputation<AstralBodyCoordinateWritable,SolarMessage, SolarMessage>{
		//
		//			@Override
		//			protected void vertexInLayerComputation(
		//					Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
		//					Iterable<SolarMessage> msgs) throws IOException {
		//				HashSet<Edge<LayeredPartitionedLongWritable, FloatWritable>> tempSack = new HashSet<Edge<LayeredPartitionedLongWritable, FloatWritable>>();
		//				HashSet<LayeredPartitionedLongWritable> chechSack = new HashSet<LayeredPartitionedLongWritable>();
		//				Iterator<Edge<LayeredPartitionedLongWritable, FloatWritable>> edgesIt = vertex.getEdges().iterator();
		//				while(edgesIt.hasNext()){
		//					Edge<LayeredPartitionedLongWritable, FloatWritable> current = edgesIt.next();
		//					if(!chechSack.contains(current.getTargetVertexId())){
		//						tempSack.add(EdgeFactory.create(current.getTargetVertexId().copy(), new FloatWritable(current.getValue().get())));
		//						chechSack.add(current.getTargetVertexId().copy());
		//					}
		//				}
		//				vertex.setEdges(tempSack);
		//
		//				MapWritable infoToUpdate = new MapWritable();
		//				infoToUpdate.put(new IntWritable(currentLayer+1), new IntWritable(1));
		//				aggregate(MultiScaleDirector.layerSizeAggregator, infoToUpdate);
		//			}
		//
		//		}

		public static class DummySolarMergerComputation extends MultiScaleComputation<AstralBodyCoordinateWritable,SolarMessage, SolarMessage>{

			@Override
			protected void vertexInLayerComputation(
					Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, FloatWritable> vertex,
					Iterable<SolarMessage> msgs) throws IOException {
				vertex.getValue().resetAssigned();
			}
		}

	}
}



