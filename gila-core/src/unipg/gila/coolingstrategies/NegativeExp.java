package unipg.gila.coolingstrategies;

/**
 * Extension of the CoolingStrategy abstract class. This class takes the exponent (called "cooling speed", given by "layout.coolingSpeed") of the negative exp. expression. 
 * @author Alessio Arleo
 *
 */
public class NegativeExp extends CoolingStrategy {

	private float coolingSpeed;
	
	public NegativeExp(String[] args) {
		super(args);
	}

	@Override
	protected void generateCoolingStrategy(String[] args) {
		coolingSpeed = Float.parseFloat(args[0]);
	}

	@Override
	public float cool(float temperature) {
		float damp = (float) Math.pow(Math.E, (-1)*coolingSpeed);
		return damp*temperature;
	}

}
