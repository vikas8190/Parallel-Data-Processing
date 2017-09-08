import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SparseColMatrixReducer extends Reducer<LongWritable,ColumnContrib, LongWritable, ColumnContribWritableList>{

	private MultipleOutputs mouts;

	public void setup(Context context){
		mouts = new MultipleOutputs(context);

	}
	
	public void reduce(LongWritable key, Iterable<ColumnContrib> value, Context context) throws IOException, InterruptedException{
		ArrayList<ColumnContrib> contribs = new ArrayList<ColumnContrib>();
		for(ColumnContrib val : value){
			contribs.add(new ColumnContrib(val.rowNo, val.totalContrib));
		}
		ColumnContrib [] contriblist;

		contriblist = contribs.toArray(new ColumnContrib[contribs.size()]);
		ColumnContribWritableList arrayWritableList = new ColumnContribWritableList();
		arrayWritableList.set(contriblist);
		mouts.write(PageRankColMatrix.MatrixFileID, new LongWritable(key.get()), arrayWritableList, PageRankColMatrix.MatrixPathPrefix);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		mouts.close();
	}
	
	
	
}

