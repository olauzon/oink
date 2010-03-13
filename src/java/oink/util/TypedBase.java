package oink.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

public abstract class TypedBase<T> {
	protected abstract T compute(T input);
	protected abstract byte getDataType();

	public T exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;

		try {
			T val = (T) input.get(0);
			return (val == null ? null : compute(val));
		} catch (Exception e) {
			throw WrappedIOException.wrap(
					"Caught exception processing input of "
							+ this.getClass().getName(), e);
		}
	}

	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		funcList.add(new FuncSpec(this.getClass().getName(), new Schema(
				new Schema.FieldSchema(null, getDataType()))));

		return funcList;
	}
}
