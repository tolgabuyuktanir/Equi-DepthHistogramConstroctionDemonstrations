import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kars on 6/27/16.
 */
public class HistogramEstimate extends EvalFunc<DataBag> {
    private long numberOfTuples=0l;
    private long numberOfBuckets=0l;
    private long numberOfSummaries=0l;
    private long numberOfSummaryTuples=0l;
    private long limitOfBucketTuples=0l;
    private long numberOfWrittenBuckets=0l;
    private long numberOfWrittenSummaries=0l;
    private long limitOfNumberOfSeenTuples=0l;
    private long upperBoundOnNumberOfSeenTuples=0l;
    private long keyOfFirstBucketTuple=0l;
    private long keyOfLastTuple=0l;

    public HistogramEstimate() {
    }

    public HistogramEstimate(String numberOfBuckets) {
        this.numberOfBuckets = Long.valueOf(numberOfBuckets);
    }

    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        DataBag output= DefaultBagFactory.getInstance().newDefaultBag();
        DataBag values= (DataBag) tuple.get(0);
        List list = sumAndNumberOfSummaries(tuple);
        numberOfTuples=(long)list.get(0);
        numberOfSummaries=(long)list.get(1);
        limitOfBucketTuples = numberOfTuples / numberOfBuckets;
        limitOfNumberOfSeenTuples = limitOfBucketTuples;
        for(Tuple t:values)
        {
            if(t!=null && t.size()>0 && t.get(0)!=null &t.get(1)!=null)
            {
                if (upperBoundOnNumberOfSeenTuples == 0) {
                    keyOfFirstBucketTuple = (long)t.get(0);
                }

                if ((long)t.get(1) > 0) {
                    upperBoundOnNumberOfSeenTuples += (long)t.get(1);
                } else {
                    numberOfWrittenSummaries++;
                    keyOfLastTuple = (long)t.get(0);
                }

                if (numberOfWrittenBuckets < numberOfBuckets - 1) {
                    if (upperBoundOnNumberOfSeenTuples > limitOfNumberOfSeenTuples) {
                        output.add(TupleFactory.getInstance().newTuple(keyOfFirstBucketTuple));
                        numberOfWrittenBuckets++;
                        limitOfNumberOfSeenTuples += limitOfBucketTuples;
                        keyOfFirstBucketTuple =(long) t.get(0);
                    }
                } else {
                    if (numberOfWrittenSummaries == numberOfSummaries) {
                        output.add(TupleFactory.getInstance().newTuple(keyOfFirstBucketTuple));
                        output.add(TupleFactory.getInstance().newTuple(keyOfLastTuple));
                    }
                }
            }
        }
        return output;
    }
    static protected List sumAndNumberOfSummaries(Tuple tuple) throws ExecException {
        DataBag values = (DataBag)tuple.get(0);
        List list=new ArrayList(2);
        long sum=0l,numberOfSummaries=0l;
        for(Tuple t:values)
        {
            if(t!=null && t.size()>0 && t.get(0)!=null && t.get(1)!=null)
            {
                sum+=Long.parseLong(t.get(1).toString());
                if((long)t.get(1)==0)
                {
                    numberOfSummaries++;
                }
            }
        }
        list.add(0,sum);
        list.add(1,numberOfSummaries);
        return list;
    }
}


