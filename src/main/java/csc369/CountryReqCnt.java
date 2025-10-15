package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.RawComparator;

public class CountryReqCnt {

    public static final Class FILE_OUTPUT_KEY_CLASS = Text.class;
    public static final Class FILE_OUTPUT_VALUE_CLASS = IntWritable.class;

    public static final Class CSV_OUTPUT_KEY_CLASS = Text.class;
    public static final Class CSV_OUTPUT_VALUE_CLASS = Text.class;

    public static final Class ACCUM_OUTPUT_KEY_CLASS = Text.class;
    public static final Class ACCUM_OUTPUT_VALUE_CLASS = IntWritable.class;

    public static final Class SORT_OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class SORT_OUTPUT_VALUE_CLASS = Text.class;


    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);

    @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split(" ");
	    Text hostname = new Text();
	    hostname.set(sa[0]);
	    context.write(hostname, one);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
    
    @Override
	protected void reduce(Text word, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(word, result);
       }
    }

    public static class ReqMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text hostname = new Text();
	private Text reqCnt = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	    String[] sa = value.toString().split("\t");
    	    hostname.set(sa[0]);
    	    reqCnt.set(sa[1]);
    	    context.write(hostname, reqCnt);
    	}
    }

    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text hostname = new Text();
	private Text country = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().split(",");
    	    hostname.set(sa[0]);
    	    country.set(sa[1]);
            context.write(hostname, country);
        }
    }

	

    public static class JoinReducer extends Reducer<Text, Text, Text, IntWritable> {
	private Text country = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int numReq = 0;

		for (Text v : values){
			String val = v.toString().trim();
			try {
				numReq += Integer.parseInt(val);

			} catch (NumberFormatException e) {
				country.set(val);

			}
		}

		if (country.getLength() > 0 && numReq > 0){
        	context.write(country, new IntWritable(numReq));
		}
    }
    }

    public static class ReqCollect extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text country = new Text();
	private IntWritable numReq = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sa[] = value.toString().split("\t");
			country.set(sa[0]);
        	numReq.set(Integer.parseInt(sa[1]));
            context.write(country, numReq);
        }
    }

    public static class ReqAcc extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
    
    @Override
	protected void reduce(Text word, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(word, result);
       }
    }

	public static class SortingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private Text country = new Text();
	private IntWritable numReq = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String sa[] = value.toString().split("\t");
		country.set(sa[0]);
        numReq.set(Integer.parseInt(sa[1]));
        context.write(numReq, country);
    }
    }

    public static class SortingComparator extends WritableComparator {
    protected SortingComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
		// Inverse order (decending)
        return -((IntWritable) a).compareTo((IntWritable) b);
    }
    }

    public static class SortingReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        Iterator<Text> itr = values.iterator();

        while (itr.hasNext()) {
            context.write(itr.next(), key);
        }
    }
    }
}
