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
package unipg.gila.multi.layout;

import org.apache.giraph.conf.GiraphConfiguration;

/**
 * @author Alessio Arleo
 *
 */

public abstract class LayoutAdaptationStrategy implements AdaptationStrategy{

	public static String fixMaxKString = "multi.adaptationStrategy.maxK";
	public static String limitMaxKString = "multi.adaptationStrategy.limitMaxKOnSpanningTree";


	protected static int globalMaxK = 10;

	protected static double maxAccuracy = 0.001; 
	protected static double maxAccuracyNotch = 10; 

	protected static double minCoolingSpeed = 0.97;
	public static double minCoolingSpeedNotch = 0.01;

	protected static double minInitialTempFactor = 1;

	public LayoutAdaptationStrategy(String confString, GiraphConfiguration conf){
		globalMaxK = conf.getInt(fixMaxKString, globalMaxK);
		completeSetupFromConfString(confString);
	}    

	protected abstract void completeSetupFromConfString(String confString);

	/* (non-Javadoc)
	 * @see unipg.gila.multi.layout.AdaptationStrategy#returnCurrentInitialTempFactor(int, int, int, int)
	 */
	public double returnCurrentInitialTempFactor(int currentLayer,
			int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer) {

		if(currentLayer == 0)
			return 4.0;
		
//		if(nOfEdgesOfLayer < 500)
//			return 0.1f;
//		if(nOfEdgesOfLayer < 1000)
//			return 0.2f;
		if(nOfEdgesOfLayer < 2000)
			return 0.5f;
		return LayoutAdaptationStrategy.minInitialTempFactor;
	}

	public double returnCurrentCoolingSpeed(int currentLayer,
			int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer) {

		if(currentLayer == 0)
			return 0.88;
		
		if(nOfEdgesOfLayer < 100)
			return minCoolingSpeed;
		if(nOfEdgesOfLayer > 100000)
			return minCoolingSpeed - 6*minCoolingSpeedNotch;
		if(nOfEdgesOfLayer > 1000000)
			return minCoolingSpeed - 7*minCoolingSpeedNotch;
		return minCoolingSpeed - 5*minCoolingSpeedNotch;

		//		if(nOfEdgesOfLayer < 500)
		//			return minCoolingSpeed;
		//		if(nOfEdgesOfLayer < 1000)
		//			return minCoolingSpeed;
		//		if(nOfEdgesOfLayer < 10000)
		//			return minCoolingSpeed - minCoolingSpeedNotch;
		//		if(nOfEdgesOfLayer < 100000)
		//			return minCoolingSpeed - 2*minCoolingSpeedNotch;		
		//		if(nOfEdgesOfLayer < 1000000)
		//			return minCoolingSpeed - 3*minCoolingSpeedNotch;
		/*		if(nOfEdgesOfLayer < 1000000)
			return minCoolingSpeed - 2*minCoolingSpeedNotch;*/
		//		if(nOfEdgesOfLayer < 1000000)
		//			return minCoolingSpeed - 3*minCoolingSpeedNotch;
		//		return minCoolingSpeed - 4*minCoolingSpeedNotch; 
	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.layout.AdaptationStrategy#returnTargetAccuracyy(int, int, int, int)
	 */
	public double returnTargetAccuracy(int currentLayer, int nOfLayers,
			int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
		if(nOfEdgesOfLayer < 10000)
			return LayoutAdaptationStrategy.maxAccuracy;
		//		if(nOfEdgesOfLayer < 10000)
		//			return 0.01f;
		if(nOfEdgesOfLayer < 1000000)
			return 0.01f;
		return 0.1f;
		////		if(nOfEdgesOfLayer < 1000)
		////			return maxAccuracy;
		//		if(nOfEdgesOfLayer < 100000)
		////			return maxAccuracy;
		////		if(nOfEdgesOfLayer < 100000)
		//			return maxAccuracy;//*maxAccuracyNotch;
		//		//	      if(nOfEdgesOfLayer < 1000000)
		//		//	        return 0.01f;
		//		return maxAccuracy*Math.pow(maxAccuracyNotch, 1);
	}

	public static class DensityDrivenAdaptationStrategy extends LayoutAdaptationStrategy{

		/**
		 * @param confString
		 */
		public DensityDrivenAdaptationStrategy(String confString, GiraphConfiguration conf) {
			super(confString, conf);
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy.LayoutAdaptation#returnCurrentK(int, int, int, int)
		 */
		public int returnCurrentK(int currentLayer, int nOfLayers,
				int nOfVerticesOfLayer, int nOfEdgesOfLayer, int workers) {
			float density = nOfEdgesOfLayer/(float)nOfVerticesOfLayer;
			if(density > 1.5f)
				return 3;
			if(density > 2.5f)
				return 2;
			if(density > 4f)
				return 1;
			return LayoutAdaptationStrategy.globalMaxK;
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy#completeSetupFromConfString(java.lang.String)
		 */
		@Override
		protected void completeSetupFromConfString(String confString) {
			//NO-OP
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.AdaptationStrategy#thresholdSurpassed()
		 */
		public boolean thresholdSurpassed() {
			return true;
		}
	}

	public static class SizeDrivenAdaptationStrategy extends LayoutAdaptationStrategy{

		protected int maxK = 9;
		protected int maxKTraditional = 5;
		protected int currentLayer;

		/**
		 * @param confString
		 */
		public SizeDrivenAdaptationStrategy(String confString, GiraphConfiguration conf) {
			super(confString, conf);
		}

		/* (non-Javadoc)
		 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy.LayoutAdaptation#returnCurrentK(int, int, int, int)
		 */
		public int returnCurrentK(int currentLayer, int nOfLayers,
				int nOfVerticesOfLayer, int nOfEdgesOfLayer, int workers) {

			this.currentLayer = currentLayer;
		if(currentLayer > 0){
			if(nOfEdgesOfLayer < 200)
				return maxK;
			if(nOfEdgesOfLayer < 500)
				return 8;			
			if(nOfEdgesOfLayer < 1000)
				return 7;
			if(nOfEdgesOfLayer < 2000)
				return 6;
			if(nOfEdgesOfLayer < 10000)
				return 5;
			if(nOfEdgesOfLayer < 50000)
				return 4;			
			if(nOfEdgesOfLayer > 100000)
				return 2;
			return 3;
		}else{
			if(nOfEdgesOfLayer < 1000)
				return maxKTraditional;
			if(nOfEdgesOfLayer < 10000)
				return 4;
//			if(nOfEdgesOfLayer < 20000)
//				return ;
			if(nOfEdgesOfLayer > 1000000)
				return 2;
			//				if(nOfEdgesOfLayer > 100000)
			//					return 2;
			return 3;

		}

	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy#completeSetupFromConfString(java.lang.String)
	 */
	@Override
	protected void completeSetupFromConfString(String confString) {
		//NO-OP
	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.layout.AdaptationStrategy#thresholdSurpassed()
	 */
	public boolean thresholdSurpassed() {
		if(currentLayer == 0)
			return true;
		return false;
	}
}  

public static class DiameterDrivenAdaptationStrategy extends LayoutAdaptationStrategy{

	/**
	 * @param confString
	 */
	public DiameterDrivenAdaptationStrategy(String confString, GiraphConfiguration conf) {
		super(confString, conf);
	}

	public static int maxDiameterForLayer(int currentLayer, int nOfLayers,
			int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
		return 2 + (4*(nOfLayers-currentLayer));
	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy.LayoutAdaptation#returnCurrentK(int, int, int, int)
	 */
	public int returnCurrentK(int currentLayer, int nOfLayers,
			int nOfVerticesOfLayer, int nOfEdgesOfLayer, int workers) {
		int proposedK = 2 + (4*(nOfLayers-currentLayer));
		return proposedK;
	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy#completeSetupFromConfString(java.lang.String)
	 */
	@Override
	protected void completeSetupFromConfString(String confString) {
		//NO-OP
	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.layout.AdaptationStrategy#thresholdSurpassed()
	 */
	public boolean thresholdSurpassed() {
		return false;
	}
}

public static class SizeAndDiameterDrivenAdaptationStrategy extends SizeDrivenAdaptationStrategy{

	/**
	 * @param confString
	 */
	public SizeAndDiameterDrivenAdaptationStrategy(String confString, GiraphConfiguration conf) {
		super(confString, conf);
	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy.LayoutAdaptation#returnCurrentK(int, int, int, int)
	 */
	public int returnCurrentK(int currentLayer, int nOfLayers,
			int nOfVerticesOfLayer, int nOfEdgesOfLayer, int workers) {
		return Math.min(super.returnCurrentK(currentLayer, nOfLayers, nOfVerticesOfLayer, nOfEdgesOfLayer, workers), 
				DiameterDrivenAdaptationStrategy.maxDiameterForLayer(currentLayer, nOfLayers, nOfVerticesOfLayer, nOfEdgesOfLayer));
	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy#completeSetupFromConfString(java.lang.String)
	 */
	@Override
	protected void completeSetupFromConfString(String confString) {
		//NO-OP
	}
} 

public static class SizeDiameterEnvironmentAwareDrivenAdaptationStrategy extends LayoutAdaptationStrategy{

	private AdaptationStrategy selectedStrategy;

	private int threshold;
	private boolean limitMaxK;

	private SizeDrivenAdaptationStrategy sizeStrategy;
	private DiameterDrivenAdaptationStrategy diameterStrategy;

	/**
	 * @param confString
	 */
	public SizeDiameterEnvironmentAwareDrivenAdaptationStrategy(
			String confString, GiraphConfiguration conf) {
		super(confString, conf);
		limitMaxK = conf.getBoolean(limitMaxKString, true);
		sizeStrategy = new SizeDrivenAdaptationStrategy(confString, conf);
		diameterStrategy = new DiameterDrivenAdaptationStrategy(confString, conf);
	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy.LayoutAdaptation#returnCurrentK(int, int, int, int)
	 */
	public int returnCurrentK(int currentLayer, int nOfLayers,
			int nOfVerticesOfLayer, int nOfEdgesOfLayer, int workers) {
		if((nOfEdgesOfLayer)/workers < threshold && currentLayer > 0)
			selectedStrategy = diameterStrategy;
		else
			selectedStrategy = sizeStrategy;
		int proposedK = selectedStrategy.returnCurrentK(currentLayer, nOfLayers, nOfVerticesOfLayer, nOfEdgesOfLayer, workers);
		if(limitMaxK/* && selectedStrategy.getClass().equals(DiameterDrivenAdaptationStrategy.class)*/)
			return  proposedK > LayoutAdaptationStrategy.globalMaxK ? LayoutAdaptationStrategy.globalMaxK : proposedK;
			else
				return proposedK;
	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.layout.LayoutAdaptationStrategy#completeSetupFromConfString(java.lang.String)
	 */
	@Override
	protected void completeSetupFromConfString(String confString) {
		try{
			threshold = Integer.parseInt(confString);
		}catch(Exception e){
			threshold = 1000; 
		}      
	}

	/* (non-Javadoc)
	 * @see unipg.gila.multi.layout.AdaptationStrategy#thresholdSurpassed()
	 */
	public boolean thresholdSurpassed() {
		return selectedStrategy.thresholdSurpassed();
	}
}   

}