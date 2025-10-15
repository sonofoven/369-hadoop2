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

import java.util.Set;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.List;

public class URLCountries {

    public static final Class JOIN_OUTPUT_KEY_CLASS = Text.class;
    public static final Class JOIN_OUTPUT_VALUE_CLASS = Text.class;

    public static final Class ACCUM_OUTPUT_KEY_CLASS = Text.class;
    public static final Class ACCUM_OUTPUT_VALUE_CLASS = Text.class;


    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
	private Text url = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> countries = new ArrayList<>();

		for (Text v : values){
			String val = v.toString().trim();
			if (val.startsWith("/")){
				url.set(val);
			} else {
				countries.add(val);
			}
		}

		for (String country : countries){
        	context.write(url, new Text(country));
		}
    }
    }


    public static class URLCollect extends Mapper<LongWritable, Text, Text, Text> {
	private Text url = new Text();
	private Text country = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sa[] = value.toString().split("\t");
			url.set(sa[0]);
			country.set(sa[1]);
            context.write(url, country);
        }
    }

    public static class URLAcc extends Reducer<Text, Text, Text, Text> {
	private Text countryList = new Text();
    
    @Override
	protected void reduce(Text url, Iterable<Text> countries,
			      Context context) throws IOException, InterruptedException {

			Set<String> finalCountries = new TreeSet<String>();

            Iterator<Text> itr = countries.iterator();
        
            while (itr.hasNext()){
				finalCountries.add(itr.next().toString());
            }

			StringBuilder sb = new StringBuilder();

			for (String c : finalCountries){
				if (sb.length() > 0){ 
					sb.append(", ");
				}

				sb.append(c);
			}

			countryList.set(sb.toString());
			context.write(url, countryList);
       }
    }
}
