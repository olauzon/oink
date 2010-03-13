package oink.udf.string;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * string.REPLACE implements eval function to replace part ofa string.
 * Example:
 * REGISTER common.jar
 * memberinfo = LOAD '/replicated/member_info.dat' USING VoldemortStorage;
 * filtered_memberinfo = FILTER memberinfo by (restriction is null) OR restriction == 'NONE';
 * names = LIMIT filtered_memberinfo 500;
 * names = FOREACH names GENERATE member_id, first_name, com.linkedin.pig.REPLACE(first_name,'a', 'BULLOCKS');
 * DUMP names; 
 */
public class REPLACE extends EvalFunc<String>
{
    /**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; first column is assumed to have the column to convert
     * @exception java.io.IOException
     */
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        try{
            String source = (String)input.get(0);
            String target = (String)input.get(1);
            String replacewith = (String)input.get(2);
            return source.replaceAll(target, replacewith);
        }catch(Exception e){
            System.err.println("Failed to process input; error - " + e.getMessage());
            return null;
        }
    }


    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }

}