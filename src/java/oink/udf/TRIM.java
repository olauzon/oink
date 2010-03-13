package oink.udf;

import oink.util.TypedBase;

import org.apache.pig.data.DataType;

// TODO: support chars as second param in tuple

public class TRIM extends TypedBase<String> {

	@Override
	protected String compute(String input) {
		return input.trim();
	}

	@Override
	protected byte getDataType() {
		return DataType.CHARARRAY;
	}
}
