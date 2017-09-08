import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SparseMatrixAggrMapper extends Mapper<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {	
	public void map(LongWritable key, DoubleWritable value, Context ctx) throws IOException, InterruptedException{
		ctx.write(key, value);
	}	
}