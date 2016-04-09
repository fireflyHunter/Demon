import breeze.linalg.Axis._1
import breeze.linalg.min
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{graphx, SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import spire.std.map
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.compat.Platform.currentTime
import scala.reflect.ClassTag
import scala.compat.Platform.currentTime



/**
  * Created by wuhao on 07/03/2016.
  */
object demon {
  def main(args: Array[String]): Unit = {
    //turn off the log information
    println("Start main")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //  setMaster("local")

    println("------------------------------------------------------")
    println("                    Version:1.00")
    println("------------------------------------------------------")


    //    val conf = new SparkConf()
    //      .set("spark.akka.frameSize", "2000")
    //      .set("spark.driver.maxResultSize", "50g")
    //      .setAppName("demon")
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("closenessCentrality")
    val path: String = "s10.csv"
    val sc = new SparkContext(conf)
    val data_graph = GraphLoader.edgeListFile(sc, path, numEdgePartitions = 1)
    val begin: Long = currentTime
    val neiborsRDD = data_graph.collectNeighborIds(edgeDirection = EdgeDirection.Either).collect()

    val neibors:Map[graphx.VertexId,Array[graphx.VertexId]] = (neiborsRDD.map{x=>x._1},neiborsRDD.map{x=>x._2}).zipped.map(_ -> _).toMap
    //get neibors map of input graph

    val beginTime:Double = currentTime/1000

    var overlappingSet:ArrayBuffer[ArrayBuffer[VertexId]] = ArrayBuffer[ArrayBuffer[VertexId]]()

    for(id <-data_graph.vertices.collect().map(_._1)) {

      println("processing the vertex" + id)
      val neiborNetwork = egoNetworkExtration(vertexId = id, neibors = neibors, data_graph)
      //get the neibor network of vertex

      val communityGraph = labelPropagation(neiborNetwork, 1)
      //get community of vertex 1003 using LP

      val communitySet: ArrayBuffer[ArrayBuffer[VertexId]] = getCommunitySet(data_graph, communityGraph, id)
      //split community in to a set of local community
//      overlappingSet = communitySet ++ overlappingSet
      for (x <- communitySet) {

//        overlappingSet = mergeCommunity(overlappingSet,x,0.4)
        if(x.length >= 3) {
          overlappingSet = mergeCommunity(overlappingSet,x,0.4)
        }
      }

    }
    for(x<-overlappingSet){
      println(x.mkString(" "))


    }
    val endTime:Double = currentTime/1000
    println("Excution time: "+(endTime-beginTime))



//    graph544.vertices.collect().foreach(println(_))
//    println(communityGraph.edges.collect().length + ".." +communityGraphtest.edges.collect().length)



  }

  def egoNetworkExtration(vertexId: VertexId,neibors:Map[graphx.VertexId,Array[graphx.VertexId]], dataGraph: Graph[Int,Int]): Graph[Int,Int] = {
//      val edgeArray : Array[Edge[Int]]  = .collect()(vertexId.toInt)._2
//      val neiborMap = sc.parallelize(edgeArray)
//      val input_graph = dataGraph.mapVertices((id, _) => if (id == vertexId) 0.0 else Double.PositiveInfinity)
        val vertexArray: Array[VertexId] = neibors(vertexId).distinct

        val subGraph = dataGraph.subgraph(vpred = (id,attr) => vertexArray.contains(id))
        subGraph
//      val neiborGraph = Graph.fromEdges(neiborMap,"defaultProperty").mapVertices
  }



  /**
  * @tparam ED the edge attribute type (not used in the computation)
  *
  * @param graph the graph for which to compute the community affiliation
  * @param maxSteps the number of supersteps of LPA to be performed. Because this is a static
  * implementation, the algorithm will run for exactly this many supersteps.
  *
  * @return a graph with vertex attributes containing the label of community affiliation
    **/
  def labelPropagation[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int) = {
    val lpaGraph = graph.mapVertices { case (vid, _) => vid }
    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])] = {
      Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
    }
    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
    : Map[VertexId, Long] = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
//        println(count1Val.getClass)
        i -> (count1Val + count2Val)
      }.toMap
    }
    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
      if (message.isEmpty) attr else message.maxBy(_._2)._1
    }
    val initialMessage = Map[VertexId, Long]()
    Pregel(lpaGraph, initialMessage,maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }
  /**
    * @param wholeGraph The input graph, used to make subgraph.
    *
    * @param LPgraph The community, used to be split into small local community
    * @param formerVertex The vertex which is the specified in the beginning
    *
    * @return A set of graph(local community)
    **/
//  def getCommunitySet(wholeGraph: Graph[Int,Int], LPgraph: Graph[VertexId, Int], formerVertex: VertexId):ArrayBuffer[Graph[Int,Int]]={
//    var attributeArr : ArrayBuffer[VertexId] = ArrayBuffer[VertexId]()
////    var getVertexSet : Map[Int,Array[Int]] = Map()
//    LPgraph.vertices.collect().foreach(attributeArr+=_._2)
//    attributeArr = attributeArr.distinct
//    var communitySet : ArrayBuffer[Graph[Int,Int]] = new ArrayBuffer(attributeArr.length)
//    for(x <- attributeArr){
//      var tempArr: ArrayBuffer[VertexId] = ArrayBuffer[VertexId]()
//      LPgraph.vertices.collect().foreach{ver=> if(ver._2==x){tempArr+=ver._1}}
//      tempArr += formerVertex
//      communitySet += wholeGraph.subgraph(vpred = (id,attr) => tempArr.contains(id))
//    }
//    communitySet
//  }
  def getCommunitySet(wholeGraph: Graph[Int,Int], LPgraph: Graph[VertexId, Int], formerVertex: VertexId):ArrayBuffer[ArrayBuffer[VertexId]]={
    var attributeArr : ArrayBuffer[VertexId] = ArrayBuffer[VertexId]()
    //    var getVertexSet : Map[Int,Array[Int]] = Map()
    LPgraph.vertices.collect().foreach(attributeArr+=_._2)
    attributeArr = attributeArr.distinct
    var communitySet : ArrayBuffer[ArrayBuffer[VertexId]] = new ArrayBuffer(attributeArr.length)
    for(x <- attributeArr){
      var tempArr: ArrayBuffer[VertexId] = ArrayBuffer[VertexId]()
      LPgraph.vertices.collect().foreach{ver=> if(ver._2==x){tempArr+=ver._1}}
      tempArr += formerVertex
//      communitySet += wholeGraph.subgraph(vpred = (id,attr) => tempArr.contains(id))
      communitySet += tempArr
    }
    communitySet
  }



  /**
    * @param community The local community(Graph) set, waiting to be merged.
    *
    * @param targetMember The member in the community which will be merged with all other members.
    * @param epsilon The ε factor is introduced to vary the percentage of common elements provided from each couple of
    *                communities: ε = 0 ensure that two communities are merged only if one of them is a proper subset
    *                of the other, on the other hand with a value of ε = 1 even communities that do not share a single
    *                node are merged together.
    *
    * @return A merged community.
    **/
//  def mergeCommunity(community:ArrayBuffer[Graph[Int,Int]], targetMember:Graph[Int,Int], epsilon:Double):ArrayBuffer[Graph[Int,Int]]={
//    print("origin length is:" + community.length+'\n')
//    if(community.contains(targetMember)) {
//      print("already exist")
//       community
//    }
//    else {
//      var inserted = false
//      for(member <- community) {
//        if (member != null & targetMember != null) {
//          val union = ifMerge(targetMember, member, epsilon)
//          if (union != null) {
//            print("cnm")
//            community -= member
//            community += union
//            inserted = true
//          }
//        }
//      }
//        if (!inserted) {
//          print("gtmddwh")
//          community += targetMember
//        }
//    }
//    print("final length is:" + community.length+'\n')
//   community
//  }
  def mergeCommunity(community:ArrayBuffer[ArrayBuffer[VertexId]], targetMember:ArrayBuffer[VertexId], epsilon:Double):ArrayBuffer[ArrayBuffer[VertexId]]={
    print("origin length is:" + community.length+'\n')
    if(community.contains(targetMember)) {
      print("already exist")
      community
    }
    else {
      var inserted = false
      for(member <- community) {
        if (member != null & targetMember != null) {
          val union = ifMerge(targetMember, member, epsilon)
          if (union != null) {
            print("cnm")
            community -= member
            community += union
            inserted = true
          }
        }
      }
      if (!inserted) {
        print("gtmddwh")
        community += targetMember
      }
    }
    print("final length is:" + community.length+'\n')
    community
  }

  def ifMerge(c1:ArrayBuffer[VertexId],c2:ArrayBuffer[VertexId],epsilon:Double):ArrayBuffer[VertexId]={

    val smallerLength:Double = min(c1.size,c2.size)
    val intersect:Double = c1.intersect(c2).size
    val res:Double = intersect / smallerLength
    if (res>=epsilon){
       c1.union(c2)
    }
    else{
       null
    }
  }




  //    def
//    def minusGraph(vertex: VertexRDD, graph: Graph[Int,Int]): Graph[Int,Int] ={
//
//
//
//
//  }

//  def triangleVP(id: VertexId, attr: (List[Int], Int, Int), msg: (List[Int], Int, Int)): (List[Int], Int, Int) = {
//    val stage = attr._3 + 1
//    val triangles = msg._1.length / 2
//
//    (msg._1, triangles, stage)
//  }
//
//  def triangleSM(edge: EdgeTriplet[(List[Int], Int, Int), Int]): Iterator[(VertexId, (List[Int], Int, Int))] = {
//    if (edge.srcAttr._3 < 3) {
//      val neighbor: (List[Int], List[Int]) = countNeighbor(edge.srcId.toInt, edge.dstId.toInt)
//      val neighborIntersect: Int = countNeighborIntersect(edge.dstAttr._1, edge.srcAttr._1)
//      val src = (neighbor._1, neighborIntersect, edge.srcAttr._3)
//      val dst = (neighbor._2, neighborIntersect, edge.dstAttr._3)
//      val itSrc = Iterator((edge.srcId, src))
//      val itDst = Iterator((edge.dstId, dst))
//      itSrc ++ itDst
//    } else {
//      Iterator.empty
//    }
//  }
//  def countNeighbor(srcId: Int, dstId: Int): (List[Int], List[Int]) = {
//    val dstNil = dstId.toInt :: Nil
//    val srcNil = srcId.toInt :: Nil
//    (dstNil, srcNil)
//  }
//  def countNeighborIntersect(a: List[Int], b: List[Int]): Int = {
//    //val triangleSet = a.toSet & b.toSet
//    a.intersect(b).length
//  }
//  def triangleMC(msg1: (List[Int], Int, Int), msg2: (List[Int], Int, Int)):
//  (List[Int], Int, Int) = {
//    (mergeNeighborMessage(msg1._1, msg2._1), mergeNeighborIntersect(msg1._2, msg2._2), msg1._3)
//  }
//  def triangleMC(msg1: (List[Int], Int, Int), msg2: (List[Int], Int, Int)):
//  (List[Int], Int, Int) = {
//    (mergeNeighborMessage(msg1._1, msg2._1), mergeNeighborIntersect(msg1._2, msg2._2), msg1._3)
//  }
//  def mergeNeighborMessage(a: List[Int], b: List[Int]): List[Int] = {
//    a ++ b
//  }
//  def mergeNeighborIntersect(a: Int, b: Int): Int = {
//    a + b
//  }

}