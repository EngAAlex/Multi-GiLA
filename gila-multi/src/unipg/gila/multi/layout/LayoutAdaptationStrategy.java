/**
 * 
 */
package unipg.gila.multi.layout;

/**
 * @author Alessio Arleo
 *
 */

public class LayoutAdaptationStrategy{
	
	public static int maxK = 6;
	public static float maxAccuracy = 0.0001f;
	public static float minCoolingSpeed = 0.98f;
	public static float minInitialTempFactor = 0.8f;

	
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
			if(density > 4)
				return 1;
			return LayoutAdaptationStrategy.maxK;
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.AdaptationStrategy#returnCurrentInitialTempFactor(int, int, int, int)
		 */
		public float returnCurrentInitialTempFactor(int currentLayer,
				int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
			return LayoutAdaptationStrategy.minInitialTempFactor;
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.AdaptationStrategy#returnCurrentCoolingSpeed(int, int, int, int)
		 */
		public float returnCurrentCoolingSpeed(int currentLayer,
				int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
//			float density = nOfEdgesOfLayer/(float)nOfVerticesOfLayer;
//			if(density > 1.5)
//				return 3;
//			if(density > 2.5)
//				return 2;
//			if(density > 4)
//				return 1;
			return LayoutAdaptationStrategy.minCoolingSpeed;
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.AdaptationStrategy#returnTargetAccuracyy(int, int, int, int)
		 */
		public float returnTargetAccuracyy(int currentLayer, int nOfLayers,
				int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
//			float density = nOfEdgesOfLayer/(float)nOfVerticesOfLayer;
//			if(density > 1.5)
//				return 3;
//			if(density > 2.5)
//				return 2;
//			if(density > 4)
//				return 1;
			return LayoutAdaptationStrategy.maxAccuracy;
		}
		
	}
	
	public static class SizeDrivenAdaptationStrategy implements AdaptationStrategy{

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy.LayoutAdaptation#returnCurrentK(int, int, int, int)
		 */
		public int returnCurrentK(int currentLayer, int nOfLayers,
				int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
			if(nOfEdgesOfLayer < 1000)
				return LayoutAdaptationStrategy.maxK;
			if(nOfEdgesOfLayer < 5000)
				return 5;
			if(nOfEdgesOfLayer < 10000)
				return 4;
//			if(nOfEdgesOfLayer > 50000)
//				return 3;
			if(nOfEdgesOfLayer > 100000)
				return 2;
			if(nOfEdgesOfLayer > 1000000)
				return 1;
			return 3;
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.AdaptationStrategy#returnCurrentInitialTempFactor(int, int, int, int)
		 */
		public float returnCurrentInitialTempFactor(int currentLayer,
				int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
			if(nOfEdgesOfLayer < 250)
				return 0.1f;
			if(nOfEdgesOfLayer < 500)
				return 0.2f;
			if(nOfEdgesOfLayer < 10000)
				return 0.4f;
			return LayoutAdaptationStrategy.minInitialTempFactor;
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.AdaptationStrategy#returnCurrentCoolingSpeed(int, int, int, int)
		 */
		public float returnCurrentCoolingSpeed(int currentLayer,
				int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
			if(nOfEdgesOfLayer < 500)
				return LayoutAdaptationStrategy.minCoolingSpeed;
			if(nOfEdgesOfLayer < 1500)
				return 0.96f;
			if(nOfEdgesOfLayer < 10000)
				return 0.94f;
			if(nOfEdgesOfLayer < 1000000)
				return 0.92f;
			return 0.9f;
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.AdaptationStrategy#returnTargetAccuracyy(int, int, int, int)
		 */
		public float returnTargetAccuracyy(int currentLayer, int nOfLayers,
				int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
			if(nOfEdgesOfLayer < 1000)
				return LayoutAdaptationStrategy.maxAccuracy;
			if(nOfEdgesOfLayer < 10000)
				return 0.001f;
			if(nOfEdgesOfLayer < 1000000)
				return 0.01f;
			return 0.1f;
		}
		
	}
	
	public static class SizeAndDensityDrivenAdaptationStrategy implements AdaptationStrategy{
		
		private DensityDrivenAdaptationStrategy ddas;
		private SizeDrivenAdaptationStrategy ssas;
		
		/**
		 * 
		 */
		public SizeAndDensityDrivenAdaptationStrategy() {
			ddas = new DensityDrivenAdaptationStrategy();
			ssas = new SizeDrivenAdaptationStrategy();
		}
		
		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy.LayoutAdaptation#returnCurrentK(int, int, int, int)
		 */
		public int returnCurrentK(int currentLayer, int nOfLayers,
				int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
			int proposedK = Math.max(ddas.returnCurrentK(currentLayer, nOfLayers, nOfVerticesOfLayer, nOfEdgesOfLayer), 
					ssas.returnCurrentK(currentLayer, nOfLayers, nOfVerticesOfLayer, nOfEdgesOfLayer));
			return proposedK;
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.AdaptationStrategy#returnCurrentInitialTempFactor(int, int, int, int)
		 */
		public float returnCurrentInitialTempFactor(int currentLayer,
				int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
			return ssas.returnCurrentInitialTempFactor(currentLayer, nOfLayers, nOfVerticesOfLayer, nOfEdgesOfLayer);
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.AdaptationStrategy#returnCurrentCoolingSpeed(int, int, int, int)
		 */
		public float returnCurrentCoolingSpeed(int currentLayer,
				int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
			return ssas.returnCurrentCoolingSpeed(currentLayer, nOfLayers, nOfVerticesOfLayer, nOfEdgesOfLayer);
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.AdaptationStrategy#returnTargetAccuracyy(int, int, int, int)
		 */
		public float returnTargetAccuracyy(int currentLayer, int nOfLayers,
				int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
			return ssas.returnTargetAccuracyy(currentLayer, nOfLayers, nOfVerticesOfLayer, nOfEdgesOfLayer);
		}
		
	}
}