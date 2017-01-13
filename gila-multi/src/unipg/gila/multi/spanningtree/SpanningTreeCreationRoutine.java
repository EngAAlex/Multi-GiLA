/**
 * 
 */
package unipg.gila.multi.spanningtree;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

import unipg.gila.aggregators.HighestLPLWritableAggregator;
import unipg.gila.common.coordinatewritables.AstralBodyCoordinateWritable;
import unipg.gila.common.datastructures.SpTreeEdgeValue;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.common.multi.LayeredPartitionedLongWritable;
import unipg.gila.common.multi.SolarMessage;
import unipg.gila.multi.MultiScaleComputation;
import unipg.gila.multi.MultiScaleMaster;

/**
 * @author Alessio Arleo
 *
 */
public class SpanningTreeCreationRoutine {

  public static final String maxIDAggregator = "MAX_ID_AGGREGATOR";
  MultiScaleMaster master;

  int counter;

  public void initialize(MultiScaleMaster master) throws InstantiationException, IllegalAccessException{
    this.master = master;
    this.master.registerPersistentAggregator(maxIDAggregator, HighestLPLWritableAggregator.class);
    counter = 0;
  }

  public boolean compute(){
    switch(counter){
    case(0): master.setComputation(LeaderSelection.class); counter++; return false; 
    case(1): master.setComputation(SpanningTreeBuilder.class); counter++; return false;
    default: counter=0; return true;
    }
  }

  /**
   * @author Alessio Arleo
   *
   */
  public static class LeaderSelection extends MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, SolarMessage> {

    /* (non-Javadoc)
     * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
     */
    @Override
    protected
    void
    vertexInLayerComputation(
      Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
      Iterable<SolarMessage> msgs) throws IOException {
      MapWritable mw = new MapWritable();
      mw.put(new IntWritable(vertex.getValue().getComponent()), vertex.getId());
      aggregate(maxIDAggregator, mw);
    }

  }


  /**
   * @author Alessio Arleo
   *
   */
  public static class SpanningTreeBuilder
  extends
  MultiScaleComputation<AstralBodyCoordinateWritable, SolarMessage, SolarMessage> {

    MapWritable maxIDs;

    /* (non-Javadoc)
     * @see unipg.gila.multi.MultiScaleComputation#initialize(org.apache.giraph.graph.GraphState, org.apache.giraph.comm.WorkerClientRequestProcessor, org.apache.giraph.graph.GraphTaskManager, org.apache.giraph.worker.WorkerGlobalCommUsage, org.apache.giraph.worker.WorkerContext)
     */
    @Override
    public
    void
    initialize(
      GraphState graphState,
      WorkerClientRequestProcessor<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> workerClientRequestProcessor,
      GraphTaskManager<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> graphTaskManager,
      WorkerGlobalCommUsage workerGlobalCommUsage,
      WorkerContext workerContext) {
      super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
        workerGlobalCommUsage, workerContext);
      maxIDs = getAggregatedValue(maxIDAggregator);
    }

    /* (non-Javadoc)
     * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
     */
    @Override
    protected
    void
    vertexInLayerComputation(
      Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
      Iterable<SolarMessage> msgs) throws IOException {
      LayeredPartitionedLongWritable componentMax = (LayeredPartitionedLongWritable) maxIDs.get(new IntWritable(vertex.getValue().getComponent()));
      if(vertex.getId().getId() != componentMax.getId()){
        addEdgeRequest(vertex.getId(), EdgeFactory.create(componentMax, new SpTreeEdgeValue(true))); 
        addEdgeRequest(componentMax, EdgeFactory.create(vertex.getId(), new SpTreeEdgeValue(true))); 
      }        
    }

  }

  /**
   * @author Alessio Arleo
   *
   */
  public static class SpanningTreeConsistencyEnforcerForMoons extends
  MultiScaleComputation<AstralBodyCoordinateWritable, LayoutMessage, LayoutMessage>{

    /* (non-Javadoc)
     * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
     */
    @Override
    protected
    void
    vertexInLayerComputation(
      Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
      Iterable<LayoutMessage> msgs) throws IOException {
      boolean alreadyConnected = false;
      if(vertex.getValue().isMoon()){
        Iterable<MutableEdge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> mutableEdgesIterable = vertex.getMutableEdges();
        Iterator<MutableEdge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> mutableEdges = mutableEdgesIterable.iterator();
        while(mutableEdges.hasNext()){
          MutableEdge<LayeredPartitionedLongWritable, SpTreeEdgeValue> current = mutableEdges.next();
          if(current.getValue().isSpanningTree()){
            if(!alreadyConnected){
              alreadyConnected = true;
            }else{
              sendMessage(current.getTargetVertexId(), new LayoutMessage(vertex.getId(), new double[]{0,0})); //inform the planet to disconnect the spanning tree from the oon
              getContext().getCounter(MultiScaleComputation.MESSAGES_COUNTER_GROUP, this.getClass().getName()).increment(1);
              mutableEdges.remove();
              
              //              removeEdgeRequest(vertex.getId(), current.getTargetVertexId());
              //              removeEdgeRequest(current.getTargetVertexId(), vertex.getId());              
            }
          }
        }
      }
    }
  }

  /**
   * @author Alessio Arleo
   *
   */
  public static class SpanningTreeConsistencyEnforcerForPlanets extends MultiScaleComputation<AstralBodyCoordinateWritable, LayoutMessage, LayoutMessage>{

    /* (non-Javadoc)
     * @see unipg.gila.multi.MultiScaleComputation#vertexInLayerComputation(org.apache.giraph.graph.Vertex, java.lang.Iterable)
     */
    @Override
    protected
    void
    vertexInLayerComputation(
      Vertex<LayeredPartitionedLongWritable, AstralBodyCoordinateWritable, SpTreeEdgeValue> vertex,
      Iterable<LayoutMessage> msgs) throws IOException {
      if(vertex.getValue().isPlanet()){
        HashSet<LayeredPartitionedLongWritable> edgesToRemove = new HashSet<LayeredPartitionedLongWritable>();
        Iterator<LayoutMessage> msgsIterator = msgs.iterator();
        while(msgsIterator.hasNext())
          edgesToRemove.add(msgsIterator.next().getPayloadVertex());
        if(edgesToRemove.size() == 0)
          return;
        Iterable<MutableEdge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> mutableEdgesIterable = vertex.getMutableEdges();
        Iterator<MutableEdge<LayeredPartitionedLongWritable, SpTreeEdgeValue>> mutableEdgesIterator = mutableEdgesIterable.iterator();
        while(mutableEdgesIterator.hasNext()){
          MutableEdge<LayeredPartitionedLongWritable, SpTreeEdgeValue> current = mutableEdgesIterator.next();
          if(edgesToRemove.contains(current.getTargetVertexId()) && current.getValue().isSpanningTree()){
            mutableEdgesIterator.remove();          
          }
        }
      }
    }
  }
}


