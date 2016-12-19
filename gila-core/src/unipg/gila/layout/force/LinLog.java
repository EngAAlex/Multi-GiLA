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

/**
 * @author Alessio Arleo
 *
 */
public class LinLog extends Force {

  private double attractiveForcesConstant;

  /**
   * Parameter-less constructor. It builds an internal map used to recover the
   * options from the user configuration.
   */
  public LinLog() {

  }

  /*
   * (non-Javadoc)
   * 
   * @see unipg.dafne.layout.force.Force#generateForce(java.lang.String[])
   */
  @Override
  public void generateForce(String[] args) {
    if (args[0] == "")
      attractiveForcesConstant = 25.0;
    else
      attractiveForcesConstant = Double.parseDouble(args[0]);
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
    return new double[] { attractiveForcesConstant * (deltaX / distance),
            attractiveForcesConstant * (deltaY / distance) };
  }

  /*
   * (non-Javadoc)
   * 
   * @see unipg.dafne.layout.force.Force#computeRepulsiveForce(float[], float)
   */
  @Override
  public double[] computeRepulsiveForce(double deltaX, double deltaY,
    double distance, double squareDistance, int v1Deg, int v2Deg) {
    float degProduct = v1Deg * v2Deg;
    return new double[] { degProduct * (deltaX / squareDistance),
            degProduct * (deltaY / squareDistance) };
  }
}
