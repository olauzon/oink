package oink.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Generates the variance of the values of the specified double field. This class is Algebraic in
 * implementation, so if possible the execution will be split into a local and global application.
 * Note: there are limitations in the accuracy of this algorithm, be aware of them.  See the wiki.
 * For use on sparse data (without 0s for empty values, as in a time series), see SparseVar()
 *
 * Also note: We use count, instead of count - 1, as in computing a population variance, not a sample variance
 *
 * @author Russell Jurney, based on the AVG builtin
 * @see  <a href = "http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm">
 * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm</a> and
 * <a href="ftp://reports.stanford.edu/pub/cstr/reports/cs/tr/79/773/CS-TR-79-773.pdf">
 * ftp://reports.stanford.edu/pub/cstr/reports/cs/tr/79/773/CS-TR-79-773.pdf</a><br />
 */
public class VARIANCE extends EvalFunc<Double> implements Algebraic {

    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public Double exec(Tuple input) throws IOException {
        try {
            Double sum = sum(input);
            if(sum == null) {
                // either we were handed an empty bag or a bag
                // filled with nulls - return null in this case
                return null;
            }
            double count = count(input);

            Double avg = null;
            if (count > 0)
                avg = new Double(sum / count);

            Double sqs = null;
            sqs = sumOfErrorsSquared(input, avg);

            Double var = sqs/(count);

            return var;
        } catch (ExecException ee) {
            throw ee;
        }
    }

    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermediate.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            Tuple t = mTupleFactory.newTuple(3);
            try {
                // input is a bag with one tuple containing
                // the column we are trying to avg
                DataBag bg = (DataBag) input.get(0);
                Tuple tp = bg.iterator().next();
                Double dba = (Double)tp.get(0);

                // We emit the item, a count (1) and the square of the item
                t.set(0, dba != null ? Double.valueOf(dba.toString()) : null);
                t.set(1, 1L);
                t.set(2, dba != null ? Double.valueOf(dba.toString()) * Double.valueOf(dba.toString()) : null);
                return t;
            } catch(NumberFormatException nfe) {
                // invalid input,
                // treat this input as null
                try {
                    t.set(0, null);
                    t.set(1, 1L);
                    t.set(2, null);
                } catch (ExecException e) {
                    throw e;
                }
                return t;
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing variance in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }

        }
    }

    static public class Intermediate extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                DataBag b = (DataBag)input.get(0);
                return combine(b);
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing variance in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);

            }
        }
    }

    static public class Final extends EvalFunc<Double> {
        @Override
        public Double exec(Tuple input) throws IOException {
            try {
                DataBag b = (DataBag)input.get(0);
                Tuple combined = combine(b);

                Double sum = (Double)combined.get(0);
                Double sumOfSquares = (Double)combined.get(2);
                if(sum == null) {
                    return null;
                }
                if(sumOfSquares == null) {
                    return null;
                }

                double count = (Long)combined.get(1);

                Double var = null;
                if (count > 0) {
                    var = (sumOfSquares - (sum*sum/count))/(count-1);
                }

                return var;
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing variance in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    static protected Tuple combine(DataBag values) throws ExecException {
        double sum = 0;
        long count = 0;
        double sumOfSquares = 0;

        Tuple output = mTupleFactory.newTuple(3);

        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();

            sum += (Double) t.get(0);
            count += (Long) t.get(1);
            sumOfSquares += (Double) t.get(2);

        }

        output.set(0, sum);
        output.set(1, count);
        output.set(2, sumOfSquares);

        return output;
    }

    static protected long count(Tuple input) throws ExecException {
        DataBag values = (DataBag)input.get(0);
        return values.size();
    }

    static protected Double sum(Tuple input) throws ExecException, IOException {
        DataBag values = (DataBag)input.get(0);

        // if we were handed an empty bag, return NULL
        if(values.size() == 0) {
            return null;
        }

        double sum = 0;
        boolean sawNonNull = false;
        System.out.println("Size of values: " + values.size());
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            System.out.println("Size: " + t.size());
            try{
                //System.out.println("About to Double d = (Double)t.get(0);\n");
                Double d = (Double)t.get(0);
                //System.out.println("About to if (d == null) continue;\n");
                if (d == null) continue;
                //System.out.println("About to sawNonNull = true;\n");
                sawNonNull = true;
                //System.out.println("About to sum += d;\n");
                sum += d;
                //System.out.println("Made it through one try block.\n");
            }catch(RuntimeException exp) {
                int errCode = 2103;
                String msg = "Problem while computing sum of doubles.";
                exp.printStackTrace();
                throw new ExecException(msg, errCode, PigException.BUG, exp);
            }
        }

        if(sawNonNull) {
            return new Double(sum);
        } else {
            return null;
        }
    }

    static protected Double sumOfErrorsSquared(Tuple input, double avg) throws ExecException, IOException {
        DataBag values = (DataBag)input.get(0);

        // if we were handed an empty bag, return NULL
        if(values.size() == 0) {
            return null;
        }

        double sumsq = 0;
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            try{
                Double d = (Double)t.get(0);
                if (d == null) continue;
                sawNonNull = true;
                double diff = d - avg;
                sumsq += (diff * diff);
            }catch(RuntimeException exp) {
                int errCode = 2103;
                String msg = "Problem while computing sum of doubles.";
                throw new ExecException(msg, errCode, PigException.BUG, exp);
            }
        }

        if(sawNonNull) {
            return new Double(sumsq);
        } else {
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }

}
