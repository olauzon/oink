package oink.udf;

import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * Asserts some boolean. There is a unary and a binary version.
 * 
 * The unary version just takes a boolean, and throws out a generic exception
 * message when the assertion is violated.
 * 
 * The binary version takes a String as a second argument and throws that out
 * when the assertion is violated.
 * 
 * Unfortunately, the pig interpreter doesn't recognize boolean expressions
 * nested as function arguments, so this has reverted to C-style booleans. That
 * is, the first argument should be an integer. 0 for false, anything else for
 * true.
 * 
 * Example:
 * 
 * LOAD members from '/replicated/member_info.dat';
 * FILTER members BY ASSERT((member_id == 0) ? 1 : 0, 'This should always fail, because not everyone is member_id 0' );
 * DUMP members;
 * 
 */
public class ASSERT extends FilterFunc {
	@Override
	public Boolean exec(Tuple tuple) throws IOException {
		if ((Integer) tuple.get(0) == 0) {
			if (tuple.size() > 1) {
				throw new IOException("Assertion violated: " + tuple.get(1).toString());
			} else {
				throw new IOException("Assertion violated.  What assertion, I do not know, but it was officially violated.");
			}
		} else {
			return true;
		}
	}
}
