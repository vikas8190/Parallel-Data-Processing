import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.collections.iterators.EntrySetMapIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SparseMatrixMulReducer extends Reducer<LongWritable, ColumnContrib, LongWritable, DoubleWritable> {

	private long numberOfPages;
	private int iteration;
	private HashMap<Long, Double> rowContribution;
	private ArrayList<ColumnContrib> pageContributions;
	private double localDanglingMass;
	private double previousDanglingMass;
	private MultipleOutputs mos;


	static double alpha = 0.15;
	static double oneMinusAlpha = 0.85;
	
	public void setup(Context context){		
		Configuration conf = context.getConfiguration();
		this.mos = new MultipleOutputs(context);
		this.rowContribution = new HashMap<Long, Double>();
		this.pageContributions = new ArrayList<ColumnContrib>();
		this.localDanglingMass = 0.0;	
		this.numberOfPages = conf.getLong(PageRankColMatrix.TOTALPAGES, 1);
		this.iteration = conf.getInt(PageRankColMatrix.IterationCount, 0);
		this.previousDanglingMass = conf.getDouble(PageRankColMatrix.DanglingContrib, 0);
	}
	
	public void reduce(LongWritable key, Iterable<ColumnContrib> values, Context context)
	{
		boolean contributeToDangling = false;
		double pageRank = 0.0;
		for(ColumnContrib value: values){
			if(value.isContributer){
				this.pageContributions.add(new ColumnContrib(value.rowNo, value.totalContrib));
			}
			else if(value.isPageRank){
				pageRank = value.totalContrib;
			}
			else{
				contributeToDangling = true;
			}			
		}
		// Missing Ranks for pages which do not have contributions
		if(pageRank == 0.0){
			pageRank = alpha/numberOfPages + oneMinusAlpha*(previousDanglingMass);
		}
		
		// For 1st iterations default page rank 1/n
		if(iteration == 0){
			pageRank = 1.0/numberOfPages;
		}
		this.updateContributions(pageRank,  contributeToDangling);
	}
	
	private void updateContributions(double pageRank, boolean contributeToDangling){
		if(contributeToDangling){
			this.localDanglingMass += pageRank/numberOfPages;
		}
		for(ColumnContrib value : this.pageContributions){
			this.addToRowContribution(value.rowNo, pageRank*value.totalContrib);
		}
		this.resetContributions();
	}
	
	private void resetContributions(){
		this.pageContributions.clear();		
	}
	
	private void addToRowContribution(long rowNo, double contri){
		if(this.rowContribution.containsKey(rowNo)){
			double accumulatedContri = this.rowContribution.get(rowNo);
			accumulatedContri += contri;
			this.rowContribution.put(rowNo, accumulatedContri);
		}
		else{
			this.rowContribution.put(rowNo, contri);
		}
	}
	
	private void outputToDisk(Context ctx) throws IOException, InterruptedException{
		for(Long key : this.rowContribution.keySet()){
			mos.write(PageRankColMatrix.columnContribFileID, new LongWritable(key),
					new DoubleWritable(this.rowContribution.get(key)),
					PageRankColMatrix.colContributionPathPrefic);
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		this.outputToDisk(context);
		mos.write(PageRankColMatrix.danglingMassFileID, NullWritable.get(), new DoubleWritable(this.localDanglingMass),
				PageRankColMatrix.danglingContribPathPrefix);
		mos.close();
	}
}
