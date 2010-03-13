package oink.udf;

import org.apache.pig.data.DataType;

import oink.util.TypedBase;

/**
 * returns SQRT if value is a number.
 */
public class SQRT extends TypedBase<Double>
{
	@Override
	protected Double compute(Double input){
		return Math.sqrt(input);
	}

	@Override
	protected byte getDataType() {
		return DataType.DOUBLE;
	}
}
