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
