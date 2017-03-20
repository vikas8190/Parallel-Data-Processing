/**
 * Created by vikasjanardhanan on 2/20/17.
 */
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/***
 * PageRankCombiner combines totaldelta contribution using key "~~" before sending to reducer. Also combines multiple
 * incoming contributions to given pagename into single one.
 */
public class PageRankCombiner extends Reducer<Text,PageNode,Text,PageNode>{

    /***
     * reduce : for key "~~" combine records into one which indicates dangling node contribution. Multiple incoming
     * contributions for given pagename also merged into a PageNode with the summed up contribution.
     * @param key: pagename
     * @param pageNodes: List of PageNode representing the pagename.
     * @param context: used to emit records.
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key,Iterable<PageNode> pageNodes,Context context) throws IOException,InterruptedException{
        if(key.toString().equals("~~")){
            PageNode node=new PageNode();
            node.adjList=new ArrayList<String>();
            double totalDelta=0.0;
            for(PageNode pageNode:pageNodes){
                totalDelta+=pageNode.pageRank;
            }
            node.pageRank=totalDelta;
            context.write(new Text("~~"),node);
            return;

        }
        PageNode pageNode=new PageNode();
        double contributionAcc=0.0;
        for(PageNode p:pageNodes){
            if(!p.isSink){
                pageNode.adjList=p.adjList;
                pageNode.isSink=false;
                context.write(key,pageNode);
                continue;
            }
            contributionAcc +=p.pageRank;
        }
        pageNode.isSink=true;
        pageNode.adjList=new ArrayList<String>();
        pageNode.pageRank=contributionAcc;
        context.write(key,pageNode);
    }

}
