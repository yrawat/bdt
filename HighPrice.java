import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class HighPrice {
public static class MapClass extends Mapper<LongWritable, Text, Text,DoubleWritable>
{
	public void map(LongWritable key,Text value,Context context)
	{
		try {
			String[] str = value.toString().split(",");
			double price = Double.parseDouble(str[4]);
			context.write(new Text(str[1]),new DoubleWritable(price));
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
}
public static class ReduceClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)throws IOException,InterruptedException
	{
		double price = 0;
		for(DoubleWritable val:values)
		{
			if(val.get()> price)
				price = val.get();
		}
		result.set(price);
		context.write(key,result);
	}
}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "HighPrice");
job.setJarByClass(HighPrice.class);
job.setMapperClass(MapClass.class);
job.setReducerClass(ReduceClass.class);
job.setNumReduceTasks(1);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(DoubleWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}