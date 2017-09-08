import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SparseColMatrixMapper extends Mapper <Text, PageNode, WritableComparable, Writable>{
	
	private MapWritable pageNameToNumberMap;
	
	private MultipleOutputs mouts;
	
	public void setup(Context context) throws IOException{
		
		this.pageNameToNumberMap = new MapWritable();
		mouts = new MultipleOutputs(context);

		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		URI[] paths = context.getCacheFiles();
		FileStatus[] files = fs.listStatus(new Path(paths[0].getPath()));
		this.createpageNameMap(files, conf);
	}
	
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		mouts.close();
	}
	
	public void map(Text key, PageNode value, Context context) throws IOException, InterruptedException{
		LongWritable keyToLongWritbale = (LongWritable)this.pageNameToNumberMap.get(key);
		for(String node: value.adjList){
			LongWritable nodeToLongWritbale = (LongWritable)this.pageNameToNumberMap.get(new Text(node));
			context.write(keyToLongWritbale , new ColumnContrib(nodeToLongWritbale.get(), 1.0/value.adjList.size()));
		}
		if(value.isSink || value.adjList.size() == 0){
			mouts.write(PageRankColMatrix.DanglingNodeFileID, NullWritable.get(), keyToLongWritbale, PageRankColMatrix.DanglingPathPrefix);
		}
	}
	
	private void createpageNameMap(FileStatus[] files, Configuration conf) throws IOException{
		for (int i = 0; i < files.length; i++) {
			Path currFile = files[i].getPath();
			if (currFile.toString()
					.contains(PageRankColMatrix.PageNameToNumberOutputDir + PageRankColMatrix.stringToMapInput)) {
				SequenceFile.Reader seqFileReader = new SequenceFile.Reader(conf, Reader.file(currFile));
				NullWritable key = NullWritable.get();
				MapWritable value = new MapWritable();
				while (seqFileReader.next(key, value)) {
					this.pageNameToNumberMap.putAll(value);					
				}
				seqFileReader.close();
			}
		}
	}
}
