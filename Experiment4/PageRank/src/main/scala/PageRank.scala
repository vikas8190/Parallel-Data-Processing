/**
  * Created by vikasjanardhanan on 3/14/17.
  */

import org.apache.spark.{SparkConf,SparkContext}



object PageRank {

  def main(args: Array[String]){
    val alpha:Double = 0.15
    val oneMinusAlpha:Double = 0.85
    val conf = new SparkConf().setAppName("PageRank")
	.setMaster("local[*]")
	//.setMaster("yarn")
    val sc = new SparkContext(conf)

    // Input: Takes the bz compressed file as input
    // Output: PairRDD - (PageName:String,AdjList:List[String])
    // Parses each line in the bz file and converts it to a pageName and the corresponding
    // Adjacency list representation
    val nodeGraph = sc.textFile(args(0)+"/*.bz2").
      map(line => WikiParser.parseLine(line)).//reads each line in bz file
      filter(pageInfo => pageInfo!=null).
      map(pageInfo => pageInfo.split("~~")).//splits to seperate to pagename, outlink node list
      map(splitpageInfo => (splitpageInfo(0),// converts the comma seperated outlinks to list of strings representing adjList
      if(splitpageInfo.size>1)
      splitpageInfo(1).split(", ").toList // for pages that have outgoing links
      else
      List[String]())). // for pages that dont have outgoing links
      map(pageNode => List((pageNode._1,pageNode._2)) ++ pageNode._2.
        map(adjNode => (adjNode,List[String]()))). // add dangling nodes to the graph Nodes list with  empty AdjList
      flatMap(pageNode => pageNode).
      reduceByKey((x,y) => (x++y)).// multiple entries for same pageName collapsed to one
      persist()

    // total node count in the webgraph
    val totPages = nodeGraph.count()

    // Input: PairRDD - (PageName:String,AdjList:List[String])
    // Output: PairRDD - (PageName:String,PageRank:Double)
    // Creates default initial page rank for each node in webgraph
    val pageRanks = nodeGraph.map(pageNode => (pageNode._1,1.0/totPages))

    // Input: PairRDD - (PageName:String,AdjList:List[String])
    // Output: PairRDD - (PageName:String,(AdjList:List[String],PageRank:Double))
    // Performs equijoin to generate record corresponding to default pagerank and adjList for each pageName
    var nodeGraphWithRank = nodeGraph.join(pageRanks)

    // Run 10 iterations of pageRank computation
    for(i <- 1 to 10){

      // Input: PairRDD - (PageName:String,(AdjList:List[String],PageRank:Double))
      // Output: Double
      // Does filter to get all dangling nodes and then accumulates their pageRank to calculate
      // delta for the current iteration
      val delta = nodeGraphWithRank.filter(pageNode => pageNode._2._1.length==0)
        .reduce((totDelta,pageNode) => (totDelta._1,(totDelta._2._1,totDelta._2._2+pageNode._2._2)))._2._2

      // Input: PairRDD - (PageName:String,(AdjList:List[String],PageRank:Double))
      // Output: PairRDD - (PageName:String,PageRank:Double)
      // Total inlink pageRank contribution to a PageNode accumulated and produced as (pageName,total inlink pageRank
      // contribution to pageName)
      val pageRanks = nodeGraphWithRank.values.
        map(adjListPageRank => adjListPageRank._1.
          map(pageNode => (pageNode,adjListPageRank._2/adjListPageRank._1.size))).
        flatMap(pageNode => pageNode).
        reduceByKey((x,y) => x+y)

      // Input: PairRDD - (PageName:String, AdjList:List[String]),
      //        PairRDD - PairRDD - (PageName:String,PageRank:Double)
      // Output: PairRDD - (PageName:String, PageRank:Double)
      // Calculate the pageRank associated with each PageNode for this iteration by taking into
      // account the dangling node contribution and incoming link contribution to pageNode under
      // consideration
      nodeGraphWithRank = nodeGraph.leftOuterJoin(pageRanks).
        map(u => {
         (u._1, (u._2._1, u._2._2 match {
            case None => (alpha/totPages) + ((oneMinusAlpha) * delta/totPages) //PAge with no inlink
            case Some (x:Double) => (alpha/totPages) + (oneMinusAlpha*((delta/totPages) + x)) // Page with inlink contribution
          }))
        })

    }

    // Input: PairRDD -(PageName:String, (AdjList:List[String],PageRank:Double))
    // Output: Array - (PageRank:Double, PageName:String)
    // Converts into a structure as required and outputs the top 100 pageNode records based on
    // pageRank
    val result = nodeGraphWithRank.
      map(pageNode => {(pageNode._2._2,pageNode._1)}).
      top(100)

    sc.parallelize(result,1).saveAsTextFile(args(1))
  }


}
