/**
 * Created by vikasjanardhanan on 2/20/17.
 */
import java.io.IOException;
import java.util.ArrayList;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/***
 * PageRankMapper is responsible for adding dangling node contribution from i-1 to pagerank values existing for each
 * page and then sending the new updated contribution to each of its outlinks equally.
 */
public class PageRankMapper extends Mapper<Text,PageNode,Text,PageNode> {
    /***
     * If its first run, the initial page rank is set for the pagename.otherwise pagerank of node is updated with contribution
     * from dangling nodes of the previous run. If pagename is dangling node, then "~~" key is emitted so that the dangling
     * contribution can be accumulated at reducer. The contribution to the outlinks as a result of modified pagerank of
     * current pagename is also emitted.
     * @param key : pagename
     * @param pageNode: PageNode containing details for the corresponding pagename
     * @param context: used to emit records
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Text key,PageNode pageNode,Context context) throws IOException,InterruptedException{
       boolean isFirstRun=context.getConfiguration().getBoolean(PageRank.ISFIRSTRUN,false);
       long totalPages=context.getConfiguration().getLong(PageRank.TOTALPAGES,1);
       double delta=context.getConfiguration().getDouble(PageRank.DELTA,0.0);
       double new_pageRank=0.0;
       double alpha=0.85;
       // initially set pagerank as 1.0/totalpages
       if(isFirstRun){
          new_pageRank=1.0/totalPages;
       }
       else{
           // new pagerank after summing contribution from delta of previous run
           new_pageRank = pageNode.pageRank+alpha*(delta/totalPages);
       }
       pageNode.pageRank=new_pageRank;
       context.write(key,pageNode);

       //dangling node
       if(pageNode.adjList.size()==0){
            context.write(new Text("~~"),pageNode);

       }
       else {
           // extra contribution to each outlink as a result of new modified pagerank.
           for (String outlink : pageNode.adjList) {
                PageNode node=new PageNode();
                node.adjList=new ArrayList<String>();
                node.pageRank=new_pageRank/pageNode.adjList.size();
                context.write(new Text(outlink),node);
           }
       }
    }

}
