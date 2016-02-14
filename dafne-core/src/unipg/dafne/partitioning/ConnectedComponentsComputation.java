/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unipg.dafne.partitioning;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import unipg.dafne.common.datastructures.EdgeValue;
import unipg.dafne.common.datastructures.PartitioningVertexValue;
import unipg.dafne.common.datastructures.messagetypes.MessageWritable;

/**
 * This class contains the code for the connected components discovery.
 * 
 *  It has been slightly modified to adapt to Dafne's framework but it has been left untouched for the most part. 
 *  
 *  What follows is the description taken from the original class.
 * 
 * ##################################################################
 * 
 * Implementation of the HCC algorithm that identifies connected components and
 * assigns each vertex its "component identifier" (the smallest vertex id
 * in the component)
 *
 * The idea behind the algorithm is very simple: propagate the smallest
 * vertex id along the edges to all vertices of a connected component. The
 * number of supersteps necessary is equal to the length of the maximum
 * diameter of all components + 1
 *
 * The original Hadoop-based variant of this algorithm was proposed by Kang,
 * Charalampos, Tsourakakis and Faloutsos in
 * "PEGASUS: Mining Peta-Scale Graphs", 2010
 *
 * http://www.cs.cmu.edu/~ukang/papers/PegasusKAIS.pdf
 * 
 * ####################################################################
 * 
 * @author Alessio Arleo
 */

public class ConnectedComponentsComputation extends
    AbstractComputation<LongWritable, PartitioningVertexValue, EdgeValue, LongWritable, LongWritable> {
	
	public static final String activityAggr = "ACTIVITY_AGGR";
	
  /**
   * Propagates the smallest vertex id to all neighbors. Will always choose to
   * halt and only reactivate if a smaller id has been sent to it.
   *
   * @param vertex Vertex
   * @param messages Iterator of messages from the previous superstep.
   * @throws IOException
   */
  @Override
  public void compute(
      Vertex<LongWritable, PartitioningVertexValue, EdgeValue> vertex,
      Iterable<LongWritable> messages) throws IOException {
    long currentComponent;

    // First superstep is special, because we can simply look at the neighbors
    if (getSuperstep() == 0) {
      currentComponent = vertex.getId().get();
      for (Edge<LongWritable, EdgeValue> edge : vertex.getEdges()) {
        long neighbor = edge.getTargetVertexId().get();
        if (neighbor < currentComponent) {
          currentComponent = neighbor;
        }
      }
      
      vertex.getValue().setComponent(currentComponent);
      
      // Only need to send value if it is not the own id
      
      if (currentComponent != vertex.getId().get()) {
        for (Edge<LongWritable, EdgeValue> edge : vertex.getEdges()) {
          LongWritable neighbor = edge.getTargetVertexId();
          if (neighbor.get() > currentComponent) {
            sendMessage(neighbor, new LongWritable(currentComponent));
          }
        }
      }
  	  aggregate(activityAggr, new BooleanWritable(false));
      return;
    }

    currentComponent = vertex.getValue().getComponent();
    
    boolean changed = false;
    // did we get a smaller id ?
    for (LongWritable message : messages) {
      long candidateComponent = message.get();
      if (candidateComponent < currentComponent) {
        currentComponent = candidateComponent;
        changed = true;
      }
    }

    // propagate new component id to the neighbors
    if (changed) {
    	vertex.getValue().setComponent(currentComponent);
    	sendMessageToAllEdges(vertex, new LongWritable(currentComponent));
    	aggregate(activityAggr, new BooleanWritable(false));
    }
  }
}
