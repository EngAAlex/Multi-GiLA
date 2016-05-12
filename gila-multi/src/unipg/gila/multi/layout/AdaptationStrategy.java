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

	public float returnCurrentInitialTempFactor(int currentLayer, int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer);

	public float returnCurrentCoolingSpeed(int currentLayer, int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer);
	
	public float returnTargetAccuracyy(int currentLayer, int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer);


}
