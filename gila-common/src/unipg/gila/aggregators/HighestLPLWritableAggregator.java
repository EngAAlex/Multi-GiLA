/**
 * 
 */
package unipg.gila.aggregators;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import unipg.gila.common.multi.LayeredPartitionedLongWritable;

/**
 * @author Alessio Arleo
 *
 */
public class HighestLPLWritableAggregator implements
Aggregator<MapWritable> {

  MapWritable maxesForComponents;

  /* (non-Javadoc)
   * @see org.apache.giraph.aggregators.Aggregator#aggregate(org.apache.hadoop.io.Writable)
   */
  public void aggregate(MapWritable value) {
    Iterator<Entry<Writable, Writable>> it = value.entrySet().iterator();
    while(it.hasNext()){
      Entry<Writable, Writable> current = it.next();
      if(!maxesForComponents.containsKey(current.getKey()))
        maxesForComponents.put(current.getKey(), current.getValue());
      else{
        LayeredPartitionedLongWritable newValue = (LayeredPartitionedLongWritable) current.getValue();
        LayeredPartitionedLongWritable storedValue = (LayeredPartitionedLongWritable) maxesForComponents.get(current.getKey());
        if(newValue.getId() > storedValue.getId())
          maxesForComponents.put(current.getKey(), current.getValue());
      }
    }
  }

  /* (non-Javadoc)
   * @see org.apache.giraph.aggregators.Aggregator#createInitialValue()
   */
  public MapWritable createInitialValue() {
    return new MapWritable();
  }

  /* (non-Javadoc)
   * @see org.apache.giraph.aggregators.Aggregator#getAggregatedValue()
   */
  public MapWritable getAggregatedValue() {
    return maxesForComponents;
  }

  /* (non-Javadoc)
   * @see org.apache.giraph.aggregators.Aggregator#setAggregatedValue(org.apache.hadoop.io.Writable)
   */
  public void setAggregatedValue(MapWritable value) {
    maxesForComponents = value;
  }

  /* (non-Javadoc)
   * @see org.apache.giraph.aggregators.Aggregator#reset()
   */
  public void reset() {
    maxesForComponents = createInitialValue();
  }

}
