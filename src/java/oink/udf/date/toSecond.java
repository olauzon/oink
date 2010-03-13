package oink.udf.date;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class toSecond extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		// Set the time to default or the output is in UTC
		DateTimeZone.setDefault(DateTimeZone.UTC);
		DateTime dt = new DateTime((String) input.get(0).toString());

		// Set the the second and milliseconds to 0
		DateTime result = dt.secondOfMinute().setCopy(0).millisOfSecond().setCopy(0);

		return result.toString();
	}
}