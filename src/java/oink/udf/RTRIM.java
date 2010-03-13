package oink.udf;

import oink.util.TypedBase;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.data.DataType;

// TODO: support chars as second param in tuple

/**
* Uses String.length() java function to calculate string length.
*/
public class RTRIM extends TypedBase<String> {

	@Override
	protected String compute(String input) {
		return StringUtils.stripEnd(input, " \n");
	}

	@Override
	protected byte getDataType() {
		return DataType.CHARARRAY;
	}
}
