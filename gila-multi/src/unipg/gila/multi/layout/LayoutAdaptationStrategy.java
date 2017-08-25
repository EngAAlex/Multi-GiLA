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

	
  public static int maxK = 6;
  public static double maxAccuracy = 0.001; 
  public static double minCoolingSpeed = 0.98f;
  public static double minInitialTempFactor = 0.8f;

  public LayoutAdaptationStrategy(String confString, GiraphConfiguration conf){
	maxK = conf.getInt(fixMaxKString, maxK);
    completeSetupFromConfString(confString);
  }    
  
  protected abstract void completeSetupFromConfString(String confString);
  
  /* (non-Javadoc)
   * @see unipg.gila.multi.layout.AdaptationStrategy#returnCurrentInitialTempFactor(int, int, int, int)
   */
  public double returnCurrentInitialTempFactor(int currentLayer,
    int nOfLayers, int nOfVerticesOfLayer, int nOfEdgesOfLayer) {

	    if(nOfEdgesOfLayer < 250)
	        return 0.1f;
	      if(nOfEdgesOfLayer < 500)
	        return 0.2f;
	      if(nOfEdgesOfLayer < 10000)
	        return 0.4f;
	      return LayoutAdaptationStrategy.minInitialTempFactor;
	  
	  //    return LayoutAdaptationStrategy.minInitialTempFactor;
  }

  public double returnCurrentCoolingSpeed(int currentLayer,
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
	  
	  ////    if(nOfEdgesOfLayer < 500)
////      return LayoutAdaptationStrategy.minCoolingSpeed;
//    //  if(nOfEdgesOfLayer < 1500)
//    //    return 0.96f;
////    if(nOfEdgesOfLayer < 1000)
////      return 0.95f;
////    if(nOfEdgesOfLayer < 1000)
////      return 0.94;
////    if(nOfEdgesOfLayer < 5000)
////      return 0.93;
////    if(nOfEdgesOfLayer < 10000)
////      return 0.92;
//    if(nOfEdgesOfLayer < 1000000)
//        return LayoutAdaptationStrategy.minCoolingSpeed;    	
////      return 0.90;
//    return 0.88;
  }

  /* (non-Javadoc)
   * @see unipg.gila.multi.layout.AdaptationStrategy#returnTargetAccuracyy(int, int, int, int)
   */
  public double returnTargetAccuracyy(int currentLayer, int nOfLayers,
    int nOfVerticesOfLayer, int nOfEdgesOfLayer) {
	    if(nOfEdgesOfLayer < 1000)
	        return LayoutAdaptationStrategy.maxAccuracy;
	      if(nOfEdgesOfLayer < 10000)
	        return 0.001f;
	      if(nOfEdgesOfLayer < 1000000)
	        return 0.01f;
	      return 0.1f;
	  ////    if(nOfEdgesOfLayer < 10000)
////      return LayoutAdaptationStrategy.maxAccuracy;
////    if(nOfEdgesOfLayer < 10000)
////      return 0.001f;
//    if(nOfEdgesOfLayer < 1000000)
//      return 0.01f;
//    return 0.1f;
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
      return LayoutAdaptationStrategy.maxK;
    }

    /* (non-Javadoc)
     * @see unipg.gila.multi.layout.LayoutAdaptationStrategy#completeSetupFromConfString(java.lang.String)
     */
    @Override
    protected void completeSetupFromConfString(String confString) {
      //NO-OP
    }
  }

  public static class SizeDrivenAdaptationStrategy extends LayoutAdaptationStrategy{

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
    	
        if(nOfEdgesOfLayer < 1000)
            return LayoutAdaptationStrategy.maxK;
          if(nOfEdgesOfLayer < 5000)
            return 5;
          if(nOfEdgesOfLayer < 10000)
            return 4;
          if(nOfEdgesOfLayer > 1000000)
            return 1;
          if(nOfEdgesOfLayer > 100000)
            return 2;
          return 3;
    	
//      if(nOfEdgesOfLayer < 1000)
//        return LayoutAdaptationStrategy.maxK;
////      if(nOfEdgesOfLayer < 5000)
////        return 6;
////      if(nOfEdgesOfLayer < 10000)
////        return 5;
//      //			if(nOfEdgesOfLayer > 50000)
//      //				return 3;
//      if(nOfEdgesOfLayer > 1000000)
//        return 2;
//      if(nOfEdgesOfLayer > 100000)
//        return 3;
//      return 4;
    }

    /* (non-Javadoc)
     * @see unipg.gila.multi.layout.LayoutAdaptationStrategy#completeSetupFromConfString(java.lang.String)
     */
    @Override
    protected void completeSetupFromConfString(String confString) {
      //NO-OP
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
      if((nOfEdgesOfLayer)/workers < threshold/* && currentLayer > 0*/){
        int proposedK = diameterStrategy.returnCurrentK(currentLayer, nOfLayers, nOfVerticesOfLayer, nOfEdgesOfLayer, workers);
        if(limitMaxK)
        	return  proposedK > LayoutAdaptationStrategy.maxK ? LayoutAdaptationStrategy.maxK : proposedK;
        else
        	return proposedK;
      }else
        return sizeStrategy.returnCurrentK(currentLayer, nOfLayers, nOfVerticesOfLayer, nOfEdgesOfLayer, workers);
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
  }   
    
}