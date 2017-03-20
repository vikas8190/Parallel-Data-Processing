/**
 * Created by vikasjanardhanan on 2/20/17.
 */
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/***
 * PageRankReducer Accumulates delta contribution from multiple dangling nodes and updates global counter with the
 * value so that it can be used in next run. It also calculates page rank for given pagename
 */
public class PageRankReducer extends Reducer<Text,PageNode,Text,PageNode>{

    private static double alpha=0.15;
    // To keep track of total pagerank value to check convergence.
    double sum;
    public void setup(Context context){
        sum = 0.0;
    }

    /***
     * reduce: Accumulates total delta from current run and updates global counter if the key is "~~".
     * Otherwise calculates the pagerank of the given pagename using all incoming contribution and emit record corresponding
     * to new pagerank.
     * @param key :  pagename
     * @param pageNodes: PageNodes corresponding to the pagename
     * @param context: used to emit records
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key,Iterable<PageNode> pageNodes,Context context) throws IOException,InterruptedException{
        long totalPages =  context.getConfiguration().getLong(PageRank.TOTALPAGES,1);
        //Total delta from dangling nodes.
        if(key.toString().equals("~~")){
            double totalDelta=0.0;
            for(PageNode pageNode:pageNodes){
                totalDelta+=pageNode.pageRank;
            }

            long longDelta=Double.doubleToLongBits(totalDelta);
            context.getCounter(PageRank.counter.delta).setValue(longDelta);
            return;
        }
        PageNode pageNode=new PageNode();
        pageNode.isSink=false;
        pageNode.adjList=new ArrayList<String>();
        double contributionAcc=0.0;
        // total incoming contribution for current pagename.
        for(PageNode p:pageNodes){
            if(!p.isSink){
                pageNode.adjList=p.adjList;
                continue;
            }
            contributionAcc +=p.pageRank;
        }
        // calculate new pagerank
        double new_pageRank = (alpha/totalPages)+((1.0-alpha)*(contributionAcc));
        pageNode.pageRank=new_pageRank;
        sum+=new_pageRank;
        context.write(key,pageNode);
    }
    // to check convergence
    public void cleanup(Context context){
        System.out.println("Total PageRank : " + sum);
    }

}
