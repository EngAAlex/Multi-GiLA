/**
 * 
 */
package unipg.gila.multi.layout;

/**
 * @author Alessio Arleo
 *
 */
public interface AdaptationStrategy {

	public int returnCurrentK(int currentLayer, int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer);

}
