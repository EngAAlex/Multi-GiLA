/**
 * 
 */
package unipg.gila.layout.force;

/**
 * @author Alessio Arleo
 *
 */
public class FR extends Force {
	
	private float k;
	
	/**
	 * Parameter-less constructor.
	 */
	public FR() {

	}

	/* (non-Javadoc)
	 * @see unipg.dafne.layout.force.Force#generateForce(java.lang.String[])
	 */
	@Override
	public void generateForce(String[] args, float k) {
		this.k = k;
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.layout.force.Force#computeAttractiveForce(float[], float)
	 */
	@Override
	public float computeAttractiveForce(float deltaX, float deltaY, float distance, float squareDistance, int v1Deg, int v2Deg) {
		return squareDistance/k;
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.layout.force.Force#computeRepulsiveForce(float[], float)
	 */
	@Override
	public float computeRepulsiveForce(float deltaX, float deltaY, float distance, float squareDistance, int v1Deg, int v2Deg) {
		return 1/distance;
	}

}
