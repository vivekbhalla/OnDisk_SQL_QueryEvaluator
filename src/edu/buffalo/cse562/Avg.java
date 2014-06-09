package edu.buffalo.cse562;

import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.parser.ParseException;

public class Avg {
	private final LinkedHashMap<Integer, List<Tuple>> instream;
	private final String exp;
	private final boolean isDistinct;

	public Avg(LinkedHashMap<Integer, List<Tuple>> instream, String exp,
			boolean isDistinct) {
		this.instream = instream;
		this.exp = exp;
		this.isDistinct = isDistinct;
	}

	/**
	 * @return Average value as Double
	 * @throws ParseException
	 */
	public String getAvg() throws ParseException {
		if (instream != null) {

			String s = new Sum(instream, exp, isDistinct).getSum();

			double sum = Double.parseDouble(s);

			double avg = sum
					/ new Count(instream, exp, isDistinct, null).getCount();
			DecimalFormat f = new DecimalFormat("##0.0##");
			return f.format(avg).toString();

		} else {
			return null;
		}

	}

}
