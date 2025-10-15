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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

public class CountryURLCnt {

    public static final Class JOIN_OUTPUT_KEY_CLASS = Text.class;
    public static final Class JOIN_OUTPUT_VALUE_CLASS = Text.class;

    public static final Class ACCUM_OUTPUT_KEY_CLASS = Text.class;
    public static final Class ACCUM_OUTPUT_VALUE_CLASS = IntWritable.class;

    public static final Class SORT_OUTPUT_KEY_CLASS = Text.class;
    public static final Class SORT_OUTPUT_VALUE_CLASS = Text.class;


    public static class URLMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text hostname = new Text();
	private Text url = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	    String[] sa = value.toString().split(" ");
    	    hostname.set(sa[0]);
    	    url.set(sa[6]);
    	    context.write(hostname, url);
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

	

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
	private Text country = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> urls = new ArrayList<>();

		for (Text v : values){
			String val = v.toString().trim();
			if (val.startsWith("/")){
				urls.add(val);

			} else {
				country.set(val);
			}
		}

		for (String url : urls){
        	context.write(country, new Text(url));
		}
    }
    }


    public static class URLCollect extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text countryURL = new Text();
	private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sa[] = value.toString().split("\t");
			countryURL.set(sa[0] + "\t" + sa[1]);
            context.write(countryURL, one);
        }
    }

    public static class URLAcc extends Reducer<Text, IntWritable, Text, IntWritable> {
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



	public static class SortingMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text country = new Text();
	private Text urlCount = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String sa[] = value.toString().split("\t");
		country.set(sa[0]);
        urlCount.set(sa[1] + "\t" + sa[2]);
        context.write(country, urlCount);
    }
    }


    public static class SortingReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text country, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {

		List<AbstractMap.SimpleEntry<String,Integer>> urls = new ArrayList<>();

		for (Text val : values){
			String[] parts = val.toString().split("\t");
			urls.add(new AbstractMap.SimpleEntry<>(parts[0], Integer.parseInt(parts[1])));
		}

		urls.sort((a,b) -> Integer.compare(b.getValue(), a.getValue()));

		for (AbstractMap.SimpleEntry<String,Integer> u : urls){
			context.write(country, new Text(u.getKey() + "\t" + u.getValue()));
		}
	}
    }
}
