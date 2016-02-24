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
	 * This method creates the actual Force object given the settings in the argument.
	 * @param args
	 */
	public abstract void generateForce(String args[], float k);
	
	/**
	 * Returns the x and y component of the attractive force given the x, y and module distance.
	 * 
	 * @param Delta x and delta y.
	 * @param The distance between the vertices.
	 * @return
	 */
	public abstract float computeAttractiveForce(float deltaX, float deltaY, float distance, float squareDistance, int v1Deg, int v2Deg);

	/**
	 * Returns the x and y component of the repulsive force given the x, y and module distance.
	 * 
	 * @param Delta x and delta y.
	 * @param The distance between the vertices.
	 * @return
	 */
	public abstract float computeRepulsiveForce(float deltaX, float deltaY, float distance, float squareDistance, int v1Deg, int v2Deg);

}
