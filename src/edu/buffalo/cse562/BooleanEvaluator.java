/**
 * 
 */
package edu.buffalo.cse562;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * @author Pratik
 * 
 */
public class BooleanEvaluator {

	private final SimpleDateFormat sdf;
	private Date datelhs;
	private Date daterhs;
	private final long num;
	private final double decimal;

	public BooleanEvaluator() {
		sdf = new SimpleDateFormat("yyyy-MM-dd");
		datelhs = null;
		daterhs = null;
		num = 0L;
		decimal = 0.0;

	}

	public boolean calculateBoolean(ArrayList<Metadata>[] mlist, String record) {
		boolean result = false;
		boolean flag = false;

		for (Metadata m : mlist[0]) {
			if (m != null) {
				switch (m.getOperator()) {

				case "=":
					// --------------------string = start
					// ------------------------------------//

					if (m.getDatatype().equalsIgnoreCase("string")) {
						if (!record.split("\\|")[m.getLhscol()].equals(m
								.getRhs())) {
							return false;
						} else {
							result = true;
						}

						// --------------------string = end
						// ------------------------------------//

					}

					// -----------date = --------------//
					else if (m.getDatatype().equalsIgnoreCase("date")) {
						try {
							datelhs = sdf.parse(record.split("\\|")[m
									.getLhscol()]);
							if (m.getRhscol() == -1) {
								if (!datelhs.equals(m.getRhsdate())) {
									return false;
								} else {
									result = true;
								}
							} else {
								daterhs = sdf.parse(record.split("\\|")[m
										.getRhscol()]);
								if (!datelhs.equals(daterhs)) {
									return false;
								} else {
									result = true;
								}
							}

						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						// --------------------date = end
						// ------------------------------------//

					}

					else if (m.getDatatype().equalsIgnoreCase("double")) {
						// check for double
					} else {
						// check for long

					}

					break;
				case "<>":
					// --------------------string <> start
					// ------------------------------------//

					if (m.getDatatype().equalsIgnoreCase("string")) {
						if (record.split("\\|")[m.getLhscol()].equals(m
								.getRhs())) {
							return false;
						} else {
							result = true;
						}

					}
					// --------------------string <> end
					// ------------------------------------//

					// -----------date <> --------------//
					else if (m.getDatatype().equalsIgnoreCase("date")) {
						try {
							datelhs = sdf.parse(record.split("\\|")[m
									.getLhscol()]);
							if (m.getRhscol() == -1) {
								if (datelhs.equals(m.getRhsdate())) {
									return false;
								} else {
									result = true;
								}
							} else {
								daterhs = sdf.parse(record.split("\\|")[m
										.getRhscol()]);
								if (datelhs.equals(daterhs)) {
									return false;
								} else {
									result = true;
								}
							}

						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						// --------------------date <> end
						// ------------------------------------//

					} else if (m.getDatatype().equalsIgnoreCase("double")) {
						// check for double
					} else {
						// check for long

					}

					break;

				case ">":

					// -----------date > --------------//
					if (m.getDatatype().equalsIgnoreCase("date")) {
						try {
							datelhs = sdf.parse(record.split("\\|")[m
									.getLhscol()]);
							if (m.getRhscol() == -1) {
								if (!datelhs.after(m.getRhsdate())) {
									return false;
								} else {
									result = true;
								}
							} else {
								daterhs = sdf.parse(record.split("\\|")[m
										.getRhscol()]);
								if (!datelhs.after(daterhs)) {
									return false;
								} else {
									result = true;
								}
							}

						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						// --------------------date > end
						// ------------------------------------//

					} else if (m.getDatatype().equalsIgnoreCase("double")) {
						// check for double
					} else {
						// check for long

					}

					break;
				case "<":

					// -----------date < --------------//
					if (m.getDatatype().equalsIgnoreCase("date")) {
						try {
							datelhs = sdf.parse(record.split("\\|")[m
									.getLhscol()]);
							if (m.getRhscol() == -1) {
								if (!datelhs.before(m.getRhsdate())) {
									return false;
								} else {
									result = true;
								}
							} else {
								daterhs = sdf.parse(record.split("\\|")[m
										.getRhscol()]);
								if (!datelhs.before(daterhs)) {
									return false;
								} else {
									result = true;
								}
							}

						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						// --------------------date < end
						// ------------------------------------//

					} else if (m.getDatatype().equalsIgnoreCase("double")) {
						// check for double
					} else {
						// check for long

					}

					break;
				case "<=":

					// -----------date <= --------------//
					if (m.getDatatype().equalsIgnoreCase("date")) {
						try {
							datelhs = sdf.parse(record.split("\\|")[m
									.getLhscol()]);
							if (m.getRhscol() == -1) {

								if (!datelhs.before(m.getRhsdate())) {
									if (!datelhs.equals(m.getRhsdate())) {
										return false;
									} else {
										result = true;
									}
								} else {
									result = true;
								}
							} else {
								daterhs = sdf.parse(record.split("\\|")[m
										.getRhscol()]);
								if (!datelhs.before(daterhs)) {
									if (!datelhs.equals(daterhs)) {

										return false;
									} else {
										result = true;
									}

								} else {
									result = true;
								}
							}

						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						// --------------------date <=end
						// ------------------------------------//

					} else if (m.getDatatype().equalsIgnoreCase("double")) {
						// check for double
					} else {
						// check for long

					}

					break;
				case ">=":

					// -----------date >= --------------//
					if (m.getDatatype().equalsIgnoreCase("date")) {
						try {
							datelhs = sdf.parse(record.split("\\|")[m
									.getLhscol()]);
							if (m.getRhscol() == -1) {
								if (!datelhs.after(m.getRhsdate())) {
									if (!datelhs.equals(m.getRhsdate())) {
										return false;
									} else {
										result = true;
									}
								} else {

									result = true;
								}
							} else {
								daterhs = sdf.parse(record.split("\\|")[m
										.getRhscol()]);

								if (!datelhs.after(daterhs)) {
									if (!datelhs.equals(daterhs)) {
										return false;
									} else {
										result = true;
									}
								} else {
									result = true;
								}
							}

						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						// --------------------date >=end
						// ------------------------------------//

					} else if (m.getDatatype().equalsIgnoreCase("double")) {
						// check for double
					} else {
						// check for long

					}

					break;

				}// AND switch case

			}// metadata and null

			// -------------------OR
			// STARTS---------------------------------------//

			else {// call orlist

				int count = mlist[1].size();
				for (Metadata mor : mlist[1]) {

					switch (mor.getOperator()) {

					case "=":
						// --------------------string = start
						// ------------------------------------//

						if (mor.getDatatype().equalsIgnoreCase("string")) {
							if (record.split("\\|")[mor.getLhscol()].equals(mor
									.getRhs())) {
								result = true;
								flag = true;
								break;
							}
						}
						// --------------------string = end
						// ------------------------------------//

						// -----------date = --------------//
						else if (mor.getDatatype().equalsIgnoreCase("date")) {
							try {
								datelhs = sdf.parse(record.split("\\|")[mor
										.getLhscol()]);
								if (mor.getRhscol() == -1) {
									if (datelhs.equals(mor.getRhsdate())) {

										result = true;
										flag = true;
										break;

									}
								} else {
									daterhs = sdf.parse(record.split("\\|")[mor
											.getRhscol()]);
									if (datelhs.equals(daterhs)) {
										result = true;
										flag = true;
										break;
									}
								}

							} catch (ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							// --------------------date = end
							// ------------------------------------//

						}

						else if (mor.getDatatype().equalsIgnoreCase("double")) {
							// check for double
						} else {
							// check for long

						}

						break;

					case "<>":
						// --------------------string <> start
						// ------------------------------------//

						if (mor.getDatatype().equalsIgnoreCase("string")) {
							if (!record.split("\\|")[mor.getLhscol()]
									.equals(mor.getRhs())) {
								result = true;
								flag = true;
								break;

							}

						}
						// --------------------string <> end
						// ------------------------------------//

						// -----------date <> --------------//
						else if (mor.getDatatype().equalsIgnoreCase("date")) {
							try {
								datelhs = sdf.parse(record.split("\\|")[mor
										.getLhscol()]);
								if (mor.getRhscol() == -1) {
									if (!datelhs.equals(mor.getRhsdate())) {
										result = true;
										flag = true;
										break;
									}
								} else {
									daterhs = sdf.parse(record.split("\\|")[mor
											.getRhscol()]);
									if (!datelhs.equals(daterhs)) {
										result = true;
										flag = true;
										break;
									}
								}

							} catch (ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							// --------------------date <> end
							// ------------------------------------//

						} else if (mor.getDatatype().equalsIgnoreCase("double")) {
							// check for double
						} else {
							// check for long

						}

						break;

					case ">":

						// -----------date > --------------//
						if (mor.getDatatype().equalsIgnoreCase("date")) {
							try {
								datelhs = sdf.parse(record.split("\\|")[mor
										.getLhscol()]);
								if (mor.getRhscol() == -1) {
									if (datelhs.after(mor.getRhsdate())) {
										result = true;
										flag = true;
										break;
									}
								} else {
									daterhs = sdf.parse(record.split("\\|")[mor
											.getRhscol()]);
									if (datelhs.after(daterhs)) {
										result = true;
										flag = true;
										break;
									}
								}

							} catch (ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							// --------------------date > end
							// ------------------------------------//

						} else if (mor.getDatatype().equalsIgnoreCase("double")) {
							// check for double
						} else {
							// check for long

						}

						break;
					case "<":

						// -----------date < --------------//
						if (mor.getDatatype().equalsIgnoreCase("date")) {
							try {
								datelhs = sdf.parse(record.split("\\|")[mor
										.getLhscol()]);
								if (mor.getRhscol() == -1) {
									if (datelhs.before(mor.getRhsdate())) {
										result = true;
										flag = true;
										break;
									}
								} else {
									daterhs = sdf.parse(record.split("\\|")[mor
											.getRhscol()]);
									if (datelhs.before(daterhs)) {
										result = true;
										flag = true;
										break;
									}
								}

							} catch (ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							// --------------------date < end
							// ------------------------------------//

						} else if (mor.getDatatype().equalsIgnoreCase("double")) {
							// check for double
						} else {
							// check for long

						}

						break;
					case "<=":

						// -----------date <= --------------//
						if (mor.getDatatype().equalsIgnoreCase("date")) {
							try {
								datelhs = sdf.parse(record.split("\\|")[mor
										.getLhscol()]);
								if (mor.getRhscol() == -1) {
									if (datelhs.equals(mor.getRhsdate())
											|| datelhs.before(mor.getRhsdate())) {
										result = true;
										flag = true;
										break;
									}
								} else {
									daterhs = sdf.parse(record.split("\\|")[mor
											.getRhscol()]);
									if (datelhs.equals(daterhs)
											|| datelhs.before(daterhs)) {
										result = true;
										flag = true;
										break;
									}
								}

							} catch (ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							// --------------------date <=end
							// ------------------------------------//

						} else if (mor.getDatatype().equalsIgnoreCase("double")) {
							// check for double
						} else {
							// check for long

						}

						break;
					case ">=":

						// -----------date >= --------------//
						if (mor.getDatatype().equalsIgnoreCase("date")) {
							try {
								datelhs = sdf.parse(record.split("\\|")[mor
										.getLhscol()]);
								if (mor.getRhscol() == -1) {
									if (datelhs.equals(mor.getRhsdate())
											|| datelhs.after(mor.getRhsdate())) {
										result = true;
										flag = true;
										break;
									}
								} else {
									daterhs = sdf.parse(record.split("\\|")[mor
											.getRhscol()]);
									if (datelhs.equals(daterhs)
											|| datelhs.after(daterhs)) {
										result = true;
										flag = true;
										break;
									}
								}

							} catch (ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							// --------------------date >=end
							// ------------------------------------//

						} else if (mor.getDatatype().equalsIgnoreCase("double")) {
							// check for double
						} else {
							// check for long

						}

						break;

					}// OR switch case
					if (flag) {
						flag = false;
						break;
					}
					count--;
					if (count == 0) {
						return false;
					}
				}// inner OR for

			}// metadata (else loop) OR ends
		}// outer AND for

		return result;
	}
}
