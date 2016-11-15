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
package unipg.gila.multi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.gila.common.datastructures.SpTreeEdgeValue;
import unipg.gila.common.datastructures.messagetypes.MessageWritable;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.multi.coarseners.InterLayerCommunicationUtils.MergerToPlacerDummyComputation;
import unipg.gila.multi.coarseners.SolarMergerRoutine;

public abstract class MultiScaleComputation<Z extends Writable, P extends MessageWritable, T extends MessageWritable> extends
AbstractComputation<LayeredPartitionedLongWritable, Z, SpTreeEdgeValue, P, T> {

  //LOGGER
  public static final String multiscaleLogString = "multi.showLog";

  Logger log = Logger.getLogger(MultiScaleComputation.class);

  protected int currentLayer;
  private boolean showLog;

  @Override	
  public void compute(
    Vertex<LayeredPartitionedLongWritable, Z, SpTreeEdgeValue> vertex,
    Iterable<P> msgs) throws IOException {
    if(vertex.getId().getLayer() != currentLayer)
      return;
    else{
      if(showLog){
        log.info("I'm " + vertex.getId());
      }
      vertexInLayerComputation(vertex, msgs);
    }
  }

  @Override
  public void initialize(
    GraphState graphState,
    WorkerClientRequestProcessor<LayeredPartitionedLongWritable, Z, SpTreeEdgeValue> workerClientRequestProcessor,
    GraphTaskManager<LayeredPartitionedLongWritable, Z, SpTreeEdgeValue> graphTaskManager,
    WorkerGlobalCommUsage workerGlobalCommUsage,
    WorkerContext workerContext) {
    super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
      workerGlobalCommUsage, workerContext);
    currentLayer = ((IntWritable)getAggregatedValue(SolarMergerRoutine.currentLayer)).get();
    showLog = getConf().getBoolean(multiscaleLogString, false);
  }

  protected abstract void vertexInLayerComputation(Vertex<LayeredPartitionedLongWritable, Z, SpTreeEdgeValue> vertex,
    Iterable<P> msgs) throws IOException;

  /* (non-Javadoc)
   * @see org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable#getConf()
   */
  @SuppressWarnings("unchecked")
  public ImmutableClassesGiraphConfiguration<LayeredPartitionedLongWritable, Writable, SpTreeEdgeValue> getSpecialConf() {
    // TODO Auto-generated method stub
    return (ImmutableClassesGiraphConfiguration<LayeredPartitionedLongWritable, Writable, SpTreeEdgeValue>) super.getConf();
  }

  public void sendMessageWithWeight(Vertex<LayeredPartitionedLongWritable, Z, SpTreeEdgeValue> vertex,
    LayeredPartitionedLongWritable id, T msg){
    //		MessageWritable w = (MessageWritable) msg;
    msg.addToWeight((vertex.getEdgeValue(id)).getValue());
    //		log.info("sendind " + msg);		
    sendMessage(id, msg);
  }

  /**
   * 
   */
  public void sendMessageToMultipleEdgesWithWeight(Vertex<LayeredPartitionedLongWritable, Z, SpTreeEdgeValue> vertex, Iterator<LayeredPartitionedLongWritable> vertexIdIterator, T message) {
    while(vertexIdIterator.hasNext()){
      MessageWritable messageCopy = message.copy();
      sendMessageWithWeight(vertex, vertexIdIterator.next(), (T)messageCopy);
    }
  }

  /**
   * 
   */
  public void sendMessageToAllEdgesWithWeight(
    Vertex<LayeredPartitionedLongWritable, Z, SpTreeEdgeValue> vertex,
    T message) {
    Iterator<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> edges = vertex.getEdges().iterator();
    while(edges.hasNext()){			
      Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue> currentEdge = edges.next();
      LayeredPartitionedLongWritable currentID = currentEdge.getTargetVertexId();
      if(currentID.getLayer() == currentLayer && !currentEdge.getValue().isSpanningTree())
        sendMessageWithWeight(vertex, currentID, (T) message.copy());
    }
  }

  /* (non-Javadoc)
   * @see org.apache.giraph.graph.AbstractComputation#sendMessageToAllEdges(org.apache.giraph.graph.Vertex, org.apache.hadoop.io.Writable)
   */
  @Override
  public void sendMessageToAllEdges(
    Vertex<LayeredPartitionedLongWritable, Z, SpTreeEdgeValue> vertex,
    T message) {
    Iterator<Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> edges = vertex.getEdges().iterator();
    while(edges.hasNext()){
      Edge<LayeredPartitionedLongWritable, SpTreeEdgeValue> currentEdge = edges.next();
      LayeredPartitionedLongWritable currentID = currentEdge.getTargetVertexId();
      if(currentID.getLayer() == currentLayer && !currentEdge.getValue().isSpanningTree())
        sendMessage(currentID, message);
    }
  }

}

