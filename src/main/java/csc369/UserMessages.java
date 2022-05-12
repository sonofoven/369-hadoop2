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

public class UserMessages {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for User file
    public static class UserMapper extends Mapper<Text, Text, Text, Text> {
	@Override
        public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
	    String name = value.toString();
	    String out = "A\t"+name;
	    context.write(key, new Text(out));
	} 
    }

    // Mapper for messages file
    public static class MessageMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
	    String text[] = value.toString().split(",");
	    if (text.length == 2) {
		String id = text[0];
		String message = text[1];
		String out = "B\t"+ message;
		context.write(new Text(id), new Text(out));
	    }
	}
    }


    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
	    ArrayList<String> name = new ArrayList();
	    ArrayList<String> messages = new ArrayList();

	    for (Text val : values) {
		context.write(key, val);
	    }
	}
    } 


}
