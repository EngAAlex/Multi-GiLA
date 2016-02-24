package unipg.gila.coolingstrategies;

/**
 * This abstract class defines the behaviour of a cooling strategy. Its abstract methods include a custom building
 * method and a "cool" method that when called returns the cooled fraction of the initial temperature.
 * @author general
 *
 */
public abstract class CoolingStrategy {

	public CoolingStrategy(String[] args){
		generateCoolingStrategy(args);
	}
	
	/**
	 * An abstract method which takes as a parameter an array of String to tune the cooling strategy.
	 * 
	 * @param the array of arguments
	 */
	protected abstract void generateCoolingStrategy(String[] args);
	
	/**
	 * The cool method takes the current temperature as a parameter and returns the cooled one according to the law implemented in it.
	 * 
	 * @param temperature
	 * @return the cooled temperature.
	 */
	public abstract float cool(float temperature);  
	
}
