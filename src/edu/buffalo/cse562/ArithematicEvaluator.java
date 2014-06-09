/**
 * 
 */
package edu.buffalo.cse562;

/**
 * @author Pratik
 * 
 */
public class ArithematicEvaluator {
	public double calculate(String s) {
		double result = 0.0;
		try {

			s = s.replaceAll("\\(", "");
			s = s.replaceAll("\\)", "");
			String l, r;
			double left = 0.0;
			double right = 0.0;
			if (s.contains("*")) {
				l = s.split("\\*")[0].trim();
				r = s.split("\\*")[1].trim();

				left = Double.parseDouble(l);
				if (r.contains("-")) {
					String l1 = r.split("\\-")[0].trim();
					String r1 = r.split("\\-")[1].trim();
					double left1 = Double.parseDouble(l1);
					double right1 = Double.parseDouble(r1);
					right = left1 - right1;
				}
				result = left * right;
			}
		} catch (Exception e) {
			return Double.POSITIVE_INFINITY;
		}
		return result;

	}

}
