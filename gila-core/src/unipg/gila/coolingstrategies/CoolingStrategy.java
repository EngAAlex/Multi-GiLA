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
package unipg.gila.coolingstrategies;

/**
 * This abstract class defines the behaviour of a cooling strategy. Its abstract
 * methods include a custom building method and a "cool" method that when called
 * returns the cooled fraction of the initial temperature.
 * 
 * @author general
 *
 */
public abstract class CoolingStrategy {

  public CoolingStrategy(String[] args) {
    generateCoolingStrategy(args);
  }

  /**
   * An abstract method which takes as a parameter an array of String to tune
   * the cooling strategy.
   * 
   * @param args
   *          The array of arguments
   */
  protected abstract void generateCoolingStrategy(String[] args);

  /**
   * The cool method takes the current temperature as a parameter and returns
   * the cooled one according to the law implemented in it.
   * 
   * @param temperature
   *          The current temperature.
   * @return the cooled temperature according to the cooling strategy.
   */
  public abstract double cool(double temperature);

}
