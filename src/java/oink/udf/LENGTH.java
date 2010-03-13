package oink.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

/**
* Uses String.length() java function to calculate string length.
*/
public class LENGTH extends EvalFunc<Integer> {

	public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() != 1)
            return null;

        try {
            String str = (String)input.get(0);
            return (str == null) ? null : str.length();
        } catch (Exception e){
             throw WrappedIOException.wrap("Caught exception processing input of " + this.getClass().getName(), e);
        }
	}

	@Override
	public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.INTEGER));
	}
}
