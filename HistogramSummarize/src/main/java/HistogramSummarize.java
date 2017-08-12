import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Created by kars on 5/24/16.
 */
public class HistogramSummarize extends EvalFunc<DataBag>   {

    private Long numberOfBuckets=0l,numberOfTuples=0l,limitOfBucketTuples=0l,keyOfFirstBucketTuple=0l;
    private Long numberOfWrittenBuckets=0l,numberOfSeenTuples=0l,numberOfBucketTuples=0l;

    public HistogramSummarize(String buckets) {
        numberOfBuckets= Long.valueOf(buckets);
    }

    public HistogramSummarize() {
    }
    DataBag dataBag=DefaultBagFactory.getInstance().newDefaultBag();
    public DataBag exec(Tuple tuple) throws IOException {
        numberOfTuples=count(tuple);
        DataBag values= (DataBag) tuple.get(0);
        for(Tuple t:values)
        {
            if(t!=null && t.size()>0 && t.get(0)!=null)
            {
                Tuple finalOutput=TupleFactory.getInstance().newTuple(2);
                if(numberOfBucketTuples==0)
                {
                    keyOfFirstBucketTuple=(Long)t.get(0);
                }
                numberOfBucketTuples++;
                numberOfSeenTuples++;
                if(numberOfWrittenBuckets < numberOfBuckets-1)
                {
                    limitOfBucketTuples=(numberOfWrittenBuckets+1)*numberOfTuples/numberOfBuckets;
                    if(numberOfSeenTuples>=limitOfBucketTuples) {
                        finalOutput.set(0, keyOfFirstBucketTuple);
                        finalOutput.set(1,numberOfBucketTuples);
                        dataBag.add(finalOutput);
                        numberOfWrittenBuckets++;
                        numberOfBucketTuples=0l;
                    }
                }
                else
                {
                    if(numberOfSeenTuples.equals(numberOfTuples)) {
                        finalOutput.set(0, keyOfFirstBucketTuple);
                        finalOutput.set(1, numberOfBucketTuples);
                        dataBag.add(finalOutput);
                        numberOfWrittenBuckets++;
                        Tuple finalBoundary=TupleFactory.getInstance().newTuple(2);
                        finalBoundary.set(0,t.get(0));
                        finalBoundary.set(1, 0);
                        dataBag.add(finalBoundary);
                    }
                }
            }
            }
        return dataBag;
    }

    static protected long count(Tuple input) throws ExecException {
        DataBag values = (DataBag)input.get(0);
        long cnt = 0;
        for(Tuple t:values)
        {
            if (t != null && t.size() > 0 && t.get(0) != null)
                cnt ++;
        }

        return cnt;
    }
}

