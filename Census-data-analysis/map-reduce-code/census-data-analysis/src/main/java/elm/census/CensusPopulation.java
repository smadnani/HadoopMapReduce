package elm.census;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import elm.census.mapper.CensusMapper;
import elm.census.reducer.CensusReducer;


public class CensusPopulation {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Job job = Job.getInstance(new Configuration());
		job.setJobName("CensusPopulation");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CensusMapper.class);
		job.setReducerClass(CensusReducer.class);
		job.setJarByClass(CensusPopulation.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.submit();
	}
}