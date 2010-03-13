package oink.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * string.CONCAT concatenates multiple text strings together Example: REGISTER
 * common.jar memberinfo = LOAD '/replicated/member_info.dat' USING
 * VoldemortStorage; filtered_memberinfo = FILTER memberinfo by (restriction is
 * null) OR restriction == 'NONE'; names = LIMIT filtered_memberinfo 500; names
 * = FOREACH names GENERATE member_id,
 * CONCATENATE(first_name,'BULLOCKS'); DUMP names;
 */
public class CONCAT extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;

		try {
			StringBuffer sb = new StringBuffer();
			
			for(Object stringObj : input.getAll()) {
				sb.append(stringObj);
			}
			
			return sb.toString();
		} catch (Exception e) {
			System.err.println("Failed to process input; error - " + e.getMessage());
			return null;
		}
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}