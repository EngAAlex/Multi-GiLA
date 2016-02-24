package unipg.gila.utils;


/**
 * This class implements a few convenience methods.
 * 
 * @author Alessio Arleo
 *
 */
public class Toolbox { 

	/**
	 * A method to compute the square distance between two points.
	 * 
	 * @param p1 The first point.
	 * @param p2 The second point.
	 * @return The square distance.
	 */
	public static float squareModule(float[] p1, float[] p2){
		float result = (float) (Math.pow(p2[0] - p1[0],2) + Math.pow(p2[1] - p1[1], 2));
		return floatFuzzyMath(result);
	}
	
	/**
	 * This method computes the square root of the square distance.
	 * 
	 * @param p1
	 * @param p2
	 * @return The square rooted distance between two points.
	 */
	public static float computeModule(float[] p1, float[] p2){
		float result = (float) Math.sqrt(squareModule(p1, p2));
		return floatFuzzyMath(result);
	}
	

	/**
	 * A simple method to compute the module of a vector of size 2.
	 * 
	 * @param vector
	 * @return
	 */
	public static float computeModule(float[] vector) {
		return floatFuzzyMath(new Float(Math.sqrt((Math.pow(vector[0], 2) + Math.pow(vector[1], 2)))));
	}
		

	/**
	 * This method ensures that the given value is not equal to 0, returning the same given value if it is not equal to zero
	 * or a very small value otherwise.
	 * @param value
	 * @return The value itself or a very small positive value otherwise.
	 */
	public static float floatFuzzyMath(float value){
		if(value == 0)
			return new Float(0.00001);
		return value;
	}
	
}
