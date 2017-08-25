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
package unipg.gila.layout.force;

import unipg.gila.utils.Toolbox;

/**
 * @author Alessio Arleo
 *
 */
public class FR extends Force {

  /**
   * Parameter-less constructor.
   */
  public FR() {

  }

  /*
   * (non-Javadoc)
   * 
   * @see unipg.dafne.layout.force.Force#generateForce(java.lang.String[])
   */
  @Override
  public void generateForce(String[] args) {
  }

  /*
   * (non-Javadoc)
   * 
   * @see unipg.dafne.layout.force.Force#computeAttractiveForce(float[], float)
   */
  @Override
  public double[] computeAttractiveForce(double deltaX, double deltaY,
    double distance, double squareDistance, double desiredDistance,
          int v1Deg, int v2Deg) {
    return new double[] { deltaX * distance / desiredDistance,
            deltaY * distance / desiredDistance };
  }

  /*
   * (non-Javadoc)
   * 
   * @see unipg.dafne.layout.force.Force#computeRepulsiveForce(float[], float)
   */
  @Override
  public double[] computeRepulsiveForce(double deltaX, double deltaY,
    double distance, double squareDistance, int v1Deg, int v2Deg) {
    return new double[] {
            (deltaX / Toolbox.doubleFuzzyMath(squareDistance)),
            (deltaY / Toolbox.doubleFuzzyMath(squareDistance)) };
  }
}
