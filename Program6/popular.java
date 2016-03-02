import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.nio.ByteBuffer;

import com.alexholmes.json.mapreduce.MultiLineJsonInputFormat;


public class popular extends Configured implements Tool {

	public static class PopularMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text outputKey = new Text();
		private IntWritable outputValue = new IntWritable();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				JSONObject json = new JSONObject(value.toString());
				String text = json.getString("text");

				String[] words = text.split(" ");
				for (String word : words) {
					word = word.replaceAll("[^a-zA-Z0-9]", "");
					if (!word.equals("")) {
						outputKey.set(word.toLowerCase());
						outputValue.set(1);
						context.write(outputKey, outputValue);
					}
				}
			}
			catch (Exception e) {
				System.out.println(e);
			}
		}

	}

	public static class PopularReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	public static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable outputKey = new IntWritable();
		private Text outputValue = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String valueStr = value.toString();
			String[] vals = valueStr.split("\t");
			if (vals.length == 2) {
				String word = vals[0];
				String countStr = vals[1];

				int count = Integer.parseInt(countStr);
				outputKey.set(count);
				outputValue.set(word);
				context.write(outputKey, outputValue);
			}
		}

	}

	public static class CountSort extends WritableComparator {
        protected CountSort() {
            super (IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int j1, int k1, byte[] b2, int j2, int k2) {
            Integer a = ByteBuffer.wrap(b1, j1, k1).getInt();
            Integer b = ByteBuffer.wrap(b2, j2, k2).getInt();
            return a.compareTo(b) * -1;
        }
    }

	public static class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		private Text result = new Text();

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(key, val);
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf, "popular job");

		job.setJarByClass(popular.class);
		job.setMapperClass(PopularMapper.class);
		job.setReducerClass(PopularReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(MultiLineJsonInputFormat.class);
		MultiLineJsonInputFormat.setInputJsonMember(job, "text");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		Job sortJob = Job.getInstance(conf, "sort popular job");
		sortJob.setJarByClass(popular.class);
		sortJob.setMapperClass(SortMapper.class);
		sortJob.setReducerClass(SortReducer.class);
		sortJob.setOutputKeyClass(IntWritable.class);
		sortJob.setOutputValueClass(Text.class);
		sortJob.setSortComparatorClass(CountSort.class);
		//sortJob.setInputFormatClass(TextInputFormat.class);
		//sortJob.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(sortJob, new Path(args[1]));
		TextOutputFormat.setOutputPath(sortJob, new Path(args[2]));

		return sortJob.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int result = ToolRunner.run(conf, new popular(), args);
		System.exit(result);
	}

}