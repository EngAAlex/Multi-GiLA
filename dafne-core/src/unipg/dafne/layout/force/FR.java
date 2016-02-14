/**
 * 
 */
package unipg.dafne.layout.force;

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
	public void generateForce(Object[] args) {
		k = (float)args[0];

	}

	/* (non-Javadoc)
	 * @see unipg.dafne.layout.force.Force#computeAttractiveForce(float[], float)
	 */
	@Override
	public float[] computeAttractiveForce(float deltaX, float deltaY, float distance, float squareDistance, int v1Deg, int v2Deg) {
		float[] attForces = new float[2];
		attForces[0] = deltaX*distance/k;
		attForces[1] = deltaY*distance/k;
		return attForces;
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.layout.force.Force#computeRepulsiveForce(float[], float)
	 */
	@Override
	public float[] computeRepulsiveForce(float deltaX, float deltaY, float distance, float squareDistance, int v1Deg, int v2Deg) {
		float[] repulsiveForces = new float[2];
		repulsiveForces[0] = deltaX*squareDistance;
		repulsiveForces[1] = deltaY*squareDistance;
		return repulsiveForces;
	}

}
