package hadoopprjt3;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ListIterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class prjt1 {

	public static class MapperOne extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private Text word = new Text("");
		private Text file = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			FileSplit split = (FileSplit) reporter.getInputSplit();

			String name = split.getPath().getName();
			//String name="filename";


			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken().toString()+":"+name);
				output.collect(new Text(word), new IntWritable(1));
			}

		}

	}// end of mapper

	public static class ReducerOne extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			int sum=0;
			
			while(values.hasNext()){
				sum ++;
				values.next();	
			}
			
			
				output.collect(new Text(key+":"+sum),null);
			

		}

	}// end of reducer
	
	public static class Mappertwo extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
@Override
public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter repoter)
		throws IOException {

			String[] str=value.toString().split(":");
	
			output.collect(new Text(str[0]), new Text(str[1]+":"+str[2]));
	

}

}

public static class Reducertwo extends MapReduceBase implements 
	Reducer<Text, Text, Text, IntWritable> {
@Override
public void reduce(Text key, Iterator<Text> values,
		OutputCollector<Text,IntWritable> output,Reporter reporter) throws IOException {
	int sum = 0;
	ArrayList<String> li=new ArrayList<String>();
	while (values.hasNext()) {
		sum++;
		li.add(values.next().toString());
	}
	for(String str:li){
		output.collect(new Text(key+":"+str+":"+sum), null);
	}
	
	li.clear();

}
}
public static class Mapperthr extends MapReduceBase implements
Mapper<LongWritable, Text, Text, Text> {
@Override
public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter repoter)
	throws IOException {

		String[] str=value.toString().split(":");

		output.collect(new Text(str[1]), new Text(str[0]+":"+str[2]+":"+str[3]));


}

}

public static class Reducerthr extends MapReduceBase implements 
Reducer<Text, Text, Text, IntWritable> {
@Override
public void reduce(Text key, Iterator<Text> values,
	OutputCollector<Text,IntWritable> output,Reporter reporter) throws IOException {
int sum = 0;
ArrayList<String> li=new ArrayList<String>();
while (values.hasNext()) {
	sum++;
	li.add(values.next().toString());
}
for(String str:li){
	String[] str1=str.split(":");
	output.collect(new Text(str1[0]+":"+key+":"+str1[1]+":"+str1[2]+":"+sum), null);
}
li.clear();

}
}
public static class Mapperfor extends MapReduceBase implements
Mapper<LongWritable, Text, Text, IntWritable> {
@Override
public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter repoter)
	throws IOException {

		output.collect(new Text(value), new IntWritable(1));

}

}

public static class Reducerfor extends MapReduceBase implements 
Reducer<Text, IntWritable, Text, Text> {
@Override
public void reduce(Text key, Iterator<IntWritable> values,
	OutputCollector<Text,Text> output,Reporter reporter) throws IOException {
BigDecimal D=new BigDecimal(176);
String[] str=key.toString().split(":");
BigDecimal n=new BigDecimal(str[2]);
BigDecimal sumn=new BigDecimal(str[4]);
BigDecimal Dn=new BigDecimal(str[3]);
BigDecimal td = null,tdidf=null;
BigDecimal idf=null;
td=n.divide(sumn,4,RoundingMode.CEILING);
td.setScale(4,RoundingMode.HALF_UP);

idf=new BigDecimal(Math.log(D.divide(Dn,4,RoundingMode.CEILING).doubleValue()));
idf.setScale(4,RoundingMode.HALF_UP);

tdidf=td.multiply(idf);
tdidf.setScale(4,RoundingMode.HALF_UP);

output.collect(new Text(str[0]+":"+str[1]),new Text(tdidf.toString()));

}
}

	public static void main(String[] args) throws Exception {
		// Path output = new Path("HadoopLab1");
		// File temp = new File(output.toString());
		Path output = new Path("Hadoopprjt1");
		File temp = new File(output.toString());

		if (temp.isDirectory() && temp.exists()) {
			FileUtils.deleteDirectory(temp);
			System.out.println("Deleting Folder");
		}

		Date start = new Date();
		System.out
				.println("Start........................... Program starting time: "
						+ start);

		//Configuration conf = new Configuration();
		System.out.println("Started Job");
		// JobConf job = new JobConf(prjt2.class);
		// JobClient job = new JobClient(conf, "MapReduce 1");

		JobConf job = new JobConf(prjt1.class);
		// job.setJobName("Driver");

		job.setJarByClass(prjt1.class);
		job.setMapperClass(prjt1.MapperOne.class);
		job.setReducerClass(prjt1.ReducerOne.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
		
		
		JobConf job1 = new JobConf(prjt1.class);
		// job.setJobName("Driver");

		job1.setJarByClass(prjt1.class);
		job1.setMapperClass(prjt1.Mappertwo.class);
		job1.setReducerClass(prjt1.Reducertwo.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		JobClient.runJob(job1);
		
		Date finish = new Date();

		System.out.println("Program ending time  " + finish);
		Long diff = (finish.getTime() - start.getTime());
		System.out.println("Driver Programs start time is: " + start
				+ " , and end time is: " + finish);
		System.out.println("Total time of execution: " + diff
				+ " milliseconds.");
		
		JobConf job2 = new JobConf(prjt1.class);
		// job.setJobName("Driver");

		job2.setJarByClass(prjt1.class);
		job2.setMapperClass(prjt1.Mapperthr.class);
		job2.setReducerClass(prjt1.Reducerthr.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		JobClient.runJob(job2);

		JobConf job3 = new JobConf(prjt1.class);
		// job.setJobName("Driver");

		job3.setJarByClass(prjt1.class);
		job3.setMapperClass(prjt1.Mapperfor.class);
		job3.setReducerClass(prjt1.Reducerfor.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, new Path(args[3]));
		FileOutputFormat.setOutputPath(job3, new Path(args[4]));
		JobClient.runJob(job3);
		
	}

}
