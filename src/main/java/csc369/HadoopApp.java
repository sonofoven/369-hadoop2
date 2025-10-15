package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
		FileSystem fd = FileSystem.get(conf);
        
		Job job1 = new Job(conf, "Job One");
		Job job2 = new Job(conf, "Job Two");
		Job job3 = new Job(conf, "Job Three");
		Job job4 = new Job(conf, "Job Four");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		fd.delete(new Path(otherArgs[3]), true);

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);

	} else if ("CountryReqCnt".equalsIgnoreCase(otherArgs[0])) { // Part 1
		// JOB 1: Extract <hostname, numRequests> from file
		job1.setMapperClass(CountryReqCnt.MapperImpl.class);
		job1.setReducerClass(CountryReqCnt.ReducerImpl.class);
		job1.setOutputKeyClass(CountryReqCnt.FILE_OUTPUT_KEY_CLASS);
	    job1.setOutputValueClass(CountryReqCnt.FILE_OUTPUT_VALUE_CLASS);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job1, new Path("job1_temp"));

		job1.waitForCompletion(true);


		// JOB 2: Extract <hostname, country> from csv file & join
		MultipleInputs.addInputPath(job2, new Path("job1_temp/part-r-00000"),
				TextInputFormat.class, CountryReqCnt.ReqMapper.class);
		MultipleInputs.addInputPath(job2, new Path(otherArgs[2]),
				TextInputFormat.class, CountryReqCnt.CountryMapper.class);

			// Combine the two data sets for a join and do a little sum per hostname
		job2.setReducerClass(CountryReqCnt.JoinReducer.class);
		job2.setOutputKeyClass(CountryReqCnt.CSV_OUTPUT_KEY_CLASS);
		job2.setOutputValueClass(CountryReqCnt.CSV_OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job2, new Path("job2_temp"));

		job2.waitForCompletion(true);

		// JOB 3: Accumulate country req counts and add them together
		job3.setMapperClass(CountryReqCnt.ReqCollect.class);
		job3.setReducerClass(CountryReqCnt.ReqAcc.class);
		job3.setOutputKeyClass(CountryReqCnt.ACCUM_OUTPUT_KEY_CLASS);
		job3.setOutputValueClass(CountryReqCnt.ACCUM_OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job3, new Path("job2_temp/part-r-00000"));
		FileOutputFormat.setOutputPath(job3, new Path("job3_temp"));

		job3.waitForCompletion(true);

		// JOB 4: Swap <country, numReq> -> <numReq, country> and sort descending
		job4.setMapperClass(CountryReqCnt.SortingMapper.class);
		job4.setReducerClass(CountryReqCnt.SortingReducer.class);
		job4.setSortComparatorClass(CountryReqCnt.SortingComparator.class);
		job4.setOutputKeyClass(CountryReqCnt.SORT_OUTPUT_KEY_CLASS);
		job4.setOutputValueClass(CountryReqCnt.SORT_OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job4, new Path("job3_temp/part-r-00000"));
		FileOutputFormat.setOutputPath(job4, new Path(otherArgs[3]));

		job4.waitForCompletion(true);

		
		fd.delete(new Path("job1_temp"), true);
		fd.delete(new Path("job2_temp"), true);
		fd.delete(new Path("job3_temp"), true);

	} else if ("CountryURLCnt".equalsIgnoreCase(otherArgs[0])) { // Part 2
		// JOB 1: Extract <hostname, country> & <hostname, URL> from csv file & join
		MultipleInputs.addInputPath(job1, new Path(otherArgs[1]),
				TextInputFormat.class, CountryURLCnt.URLMapper.class);
		MultipleInputs.addInputPath(job1, new Path(otherArgs[2]),
				TextInputFormat.class, CountryURLCnt.CountryMapper.class);

			// Combine the two data sets for a join <country, URL>
		job1.setReducerClass(CountryURLCnt.JoinReducer.class);
		job1.setOutputKeyClass(CountryURLCnt.JOIN_OUTPUT_KEY_CLASS);
		job1.setOutputValueClass(CountryURLCnt.JOIN_OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job1, new Path("job1_temp"));

		job1.waitForCompletion(true);

		// JOB 2: Count url visits per country
		job2.setMapperClass(CountryURLCnt.URLCollect.class);
		job2.setReducerClass(CountryURLCnt.URLAcc.class);
		job2.setOutputKeyClass(CountryURLCnt.ACCUM_OUTPUT_KEY_CLASS);
		job2.setOutputValueClass(CountryURLCnt.ACCUM_OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job2, new Path("job1_temp/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path("job2_temp"));

		job2.waitForCompletion(true);

		// JOB 3: Sort decending based on country then count (high->low)
		job3.setMapperClass(CountryURLCnt.SortingMapper.class);
		job3.setReducerClass(CountryURLCnt.SortingReducer.class);
		job3.setOutputKeyClass(CountryURLCnt.SORT_OUTPUT_KEY_CLASS);
		job3.setOutputValueClass(CountryURLCnt.SORT_OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job3, new Path("job2_temp/part-r-00000"));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));

		job3.waitForCompletion(true);

		
		fd.delete(new Path("job1_temp"), true);
		fd.delete(new Path("job2_temp"), true);

	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job1.waitForCompletion(true) ? 0: 1);
    }

}
