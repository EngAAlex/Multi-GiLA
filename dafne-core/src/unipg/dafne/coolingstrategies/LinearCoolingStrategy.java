package unipg.dafne.coolingstrategies;

/**
 * 
 * This class extends the abstract CoolingStrategy. It has a cooling following a linear law; its parameter is the cooling factor (determined by modifying 
 * the option "layout.coolingSpeed").
 * 
 * @author Alessio Arleo
 *
 */
public class LinearCoolingStrategy extends CoolingStrategy {

	private float coolingSpeed;
	
	public LinearCoolingStrategy(String[] args) {
		super(args);
	}

	@Override
	protected void generateCoolingStrategy(String[] args) {
		coolingSpeed = Float.parseFloat(args[0]);
	}

	@Override
	public float cool(float temperature) {
		return temperature*coolingSpeed;
	}

}
