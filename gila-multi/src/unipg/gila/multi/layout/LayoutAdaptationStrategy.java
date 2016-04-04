/**
 * 
 */
package unipg.gila.multi.layout;

/**
 * @author Alessio Arleo
 *
 */

public class LayoutAdaptationStrategy{
	
	public static int maxK = 4;
	
	public static class DensityDrivenAdaptationStrategy implements AdaptationStrategy{
		
		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy.LayoutAdaptation#returnCurrentK(int, int, int, int)
		 */
		public int returnCurrentK(int currentLayer, int nOfLayers,
				int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
			float density = nOfEdgesOfLayer/(float)nOfVerticesOfLayer;
			if(density > 1.5)
				return 3;
			if(density > 2.5)
				return 2;
			if(density > 3.5)
				return 1;
			return LayoutAdaptationStrategy.maxK;
		}
		
	}
	
	public static class SizeDrivenAdaptationStrategy implements AdaptationStrategy{

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy.LayoutAdaptation#returnCurrentK(int, int, int, int)
		 */
		public int returnCurrentK(int currentLayer, int nOfLayers,
				int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
			if(nOfEdgesOfLayer > 100000)
				return 3;
			if(nOfEdgesOfLayer > 1000000)
				return 2;
			if(nOfEdgesOfLayer > 1500000)
				return 1;
			return LayoutAdaptationStrategy.maxK;
		}
		
	}
	
	public static class SizeAndDensityDrivenAdaptationStrategy implements AdaptationStrategy{
		
		private DensityDrivenAdaptationStrategy ddas;
		private SizeAndDensityDrivenAdaptationStrategy ssas;
		
		/**
		 * 
		 */
		public SizeAndDensityDrivenAdaptationStrategy() {
			ddas = new DensityDrivenAdaptationStrategy();
			ssas = new SizeAndDensityDrivenAdaptationStrategy();
		}
		
		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy.LayoutAdaptation#returnCurrentK(int, int, int, int)
		 */
		public int returnCurrentK(int currentLayer, int nOfLayers,
				int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
			return Math.max(ddas.returnCurrentK(currentLayer, nOfLayers, nOfVerticesOfLayer, nOfEdgesOfLayer), 
					ssas.returnCurrentK(currentLayer, nOfLayers, nOfVerticesOfLayer, nOfEdgesOfLayer));
		}
		
	}
}