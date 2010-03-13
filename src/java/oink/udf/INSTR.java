package oink.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

// TODO: make this oracle compliant

/**
* Uses String.indexOf() java function to calculate string positions.
*/
public class INSTR extends EvalFunc<Integer> {

	public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;

        try {
            String needle = (String)input.get(0);
            String haystack = (String)input.get(1);
            return (needle == null || haystack == null) ? null : haystack.indexOf(needle);
        } catch (Exception e){
             throw WrappedIOException.wrap("Caught exception processing input of " + this.getClass().getName(), e);
        }
	}

	@Override
	public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.INTEGER));
	}
}
