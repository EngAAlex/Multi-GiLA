/**
 * 
 */
package unipg.dafne.layout.force;

/**
 * @author Alessio Arleo
 *
 */
public class LinLog extends Force {
	
	private float attractiveForcesConstant;
	
	/**
	 * Parameter-less constructor. It builds an internal map used to recover the options from the user configuration.
	 */
	public LinLog() {
		
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.layout.force.Force#generateForce(java.lang.String[])
	 */
	@Override
	public void generateForce(String[] args, float k) {
		attractiveForcesConstant = Float.parseFloat(args[0]);
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.layout.force.Force#computeAttractiveForce(float[], float)
	 */
	@Override
	public float computeAttractiveForce(float deltaX, float deltaY, float distance, float squareDistance, int v1Deg, int v2Deg) {
		return attractiveForcesConstant;
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.layout.force.Force#computeRepulsiveForce(float[], float)
	 */
	@Override
	public float computeRepulsiveForce(float deltaX, float deltaY, float distance, float squareDistance, int v1Deg, int v2Deg) {
		return (v1Deg+v2Deg)/distance;
	}

}
