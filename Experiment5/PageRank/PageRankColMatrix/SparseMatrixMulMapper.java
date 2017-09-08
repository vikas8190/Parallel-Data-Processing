import java.io.IOException;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class SparseMatrixMulMapper
		extends Mapper<LongWritable, ColumnContribWritableList, LongWritable, ColumnContrib> {
	
	public void map(LongWritable key, ColumnContribWritableList value, Context context)
			throws IOException, InterruptedException {
		for (Writable val : value.get()) {
			ColumnContrib colContributionval = (ColumnContrib) val;
			context.write(new LongWritable(key.get()), new ColumnContrib(colContributionval.rowNo,
					colContributionval.totalContrib));
		}	
	}
}
