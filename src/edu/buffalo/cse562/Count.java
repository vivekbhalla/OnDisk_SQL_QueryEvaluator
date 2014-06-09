package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

public class Count {
	private final LinkedHashMap<Integer, List<Tuple>> instream;
	private String exp;
	private final boolean isDistinct;
	private final String key;

	public Count(LinkedHashMap<Integer, List<Tuple>> instream, String exp,
			boolean isDistinct, String key) {
		this.instream = instream;
		this.exp = exp;
		this.key = key;
		this.isDistinct = isDistinct;
	}

	/**
	 * @return Count as Integer
	 */
	public int getCount() {
		if (instream != null) {
			if (isDistinct) {

				List<Tuple> tlist = new ArrayList<Tuple>();
				exp = exp.replaceAll("\\(", "");
				exp = exp.replaceAll("\\)", "");

				for (Integer i : instream.keySet()) {
					tlist = instream.get(i);
					for (Tuple t : tlist) {
						String val = t.getColValue(exp);
						if (val != null) {
							if (EvaluateStatement.GH.containsKey(key)) {
								EvaluateStatement.GH.get(key).add(val);
								break;
							} else {
								HashSet<String> temp = new HashSet<String>();
								temp.add(val);
								EvaluateStatement.GH.put(key, temp);
								break;
							}
						}
					}
					tlist.clear();
				}
				return 0;
			} else {

				return instream.size();
			}
		} else {
			return 0;
		}

	}

	public int combineCount() {
		int sum = 0;
		exp = exp.replaceAll("\\(", "");
		exp = exp.replaceAll("\\)", "");

		if (instream != null) {
			if (isDistinct) {

				return EvaluateStatement.GH.get(key).size();
			} else {

				for (Integer i : instream.keySet()) {

					for (Tuple t : instream.get(i)) {
						String val = t.getOsColValue(exp);
						if (val != null) {
							sum = sum + Integer.parseInt(val);
							break;
						}
					}
				}
				return sum;
			}
		} else {
			return 0;
		}

	}
}
