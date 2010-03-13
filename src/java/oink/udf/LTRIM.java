package oink.udf;

import oink.util.TypedBase;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.data.DataType;

// TODO: support chars as second param in tuple

public class LTRIM extends TypedBase<String> {

	@Override
	protected String compute(String input) {
		return StringUtils.stripStart(input, " \n");
	}

	@Override
	protected byte getDataType() {
		return DataType.CHARARRAY;
	}
}
