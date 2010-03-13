package oink.udf.date;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class UnixToISO extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		// Set the time to default or the output is in UTC
		DateTimeZone.setDefault(DateTimeZone.UTC);
		DateTime result = new DateTime((long) Long.parseLong(input.get(0).toString()));

		return result.toString();
	}
}
