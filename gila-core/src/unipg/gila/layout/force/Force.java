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
 * This abstract class models the behaviour of the different Force models.
 * 
 * @author Alessio Arleo
 *
 */
public abstract class Force {
	
	/**
	 * This method builds the force instance with the given arguments and the optimal spring length.
	 * 
	 * @param args The arguments.
	 * @param k The optimal spring length.
	 */
	public abstract void generateForce(String args[], float k);
	
	/**
	 * This method computes the attractive force module between two vertices. It will be split into its X and Y components into
	 * the Propagator class.
	 * 
	 * @param deltaX The vertices distance on the X axis.
	 * @param deltaY The vertices distance on the Y axis.
	 * @param distance The distance module.
	 * @param squareDistance The distance square module.
	 * @param v1Deg The degree of the first vertex.
	 * @param v2Deg The degree of the second vertex.
	 * @return The attractive force module exerted by v2 on v1.
	 */
	public abstract float computeAttractiveForce(float deltaX, float deltaY, float distance, float squareDistance, int v1Deg, int v2Deg);

	/**
	 * 
	 * This method computes the repulsive force module between two vertices. It will be split into its X and Y components into
	 * the Propagator class.
	 * 
	 * @param deltaX The vertices distance on the X axis.
	 * @param deltaY The vertices distance on the Y axis.
	 * @param distance The distance module.
	 * @param squareDistance The distance square module.
	 * @param v1Deg The degree of the first vertex.
	 * @param v2Deg The degree of the second vertex.
	 * @return The repulsive force module exerted by v2 on v1.
	 */
	public abstract float computeRepulsiveForce(float deltaX, float deltaY, float distance, float squareDistance, int v1Deg, int v2Deg);

}
