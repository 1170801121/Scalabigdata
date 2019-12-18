package main
import org.apache.spark.{SparkConf, SparkContext, graphx, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl

import scala.collection.mutable
// To make some of the examples work we will also need RDD

//import scala.collection.mutable.Set

object Compute extends App {


    val filename = "src/University0_0.nt"
//  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("compute").setMaster("local").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val conf = new SparkContext(sc)
    //  readOWL
    val r = conf.textFile(filename).map(_.split("\\s"))
    val v = r.map(_(0)).union(r.map(_(2))).distinct.zipWithIndex
    val graph: Graph[String, String] = Graph(v.map(_.swap), r.map(x => (x(0), (x(1), x(2))))
      .join(v).map(x => (x._2._1._2, (x._2._2, x._2._1._1)))
      .join(v).map(x => new Edge(x._2._1._1, x._2._2, x._2._1._2)))

  // Create an RDD for the vertices
//    val tpEquals: $eq$colon$eq[String, String] = scala.Predef.$eq$colon$eq$.MODULE$.tpEquals()
//    val users: VertexRDD[String] =
//      conf.parallelize(Array((3L, "student"), (7L,  "postdoc"),
//        (5L,  "prof"), (2L, "prof")))
    val rdd1: RDD[(VertexId, String)] = conf.makeRDD(Seq(
      (3L, "student"), (7L, "postdoc"),
      (5L, "prof"), (2L, "prof")
    ))
    val rdd2: RDD[Edge[String]] = conf.makeRDD(Seq(
      Edge(3L, 5L, "0"),
      Edge(3L, 7L, "0"),
      Edge(3L, 2L, "0"),
      Edge(7L, 2L, "0"),
    ))

    val ver: VertexRDD[String] = graph.vertices

//  graphx.EdgeContext[(String, attribute), String, Int]

  def propagateBFS(g:Graph[(String, attribute), String], vk:VertexId): Graph[(String, attribute), String] = {
    g.aggregateMessages[PartitionID](t => if (t.srcAttr._2.que == true && t.dstAttr._2.que == false && t.dstAttr._2.ban == false) {
      t.sendToDst(1)
      val min: VertexId = t.srcAttr._2.p + 1
      for (x <- t.dstAttr._2.hop) { //比较Qk-1和当前p
        g.mapVertices((vid, v) => if (vid == vk) {
          if (v._2.hop.contains(x)) { //找出Qk-1中src和dst距离
            if (String.valueOf(v._2.index.get(x)) + t.dstAttr._2.index.get(x) <= String.valueOf(min)) {
              t.dstAttr._2.ban = true //切掉
            }
          }
        })
      }

      //未被切掉则加入索引集中
      g.mapVertices((vid, v) => if (vid == vk) {
        if (v._2.ban == false) {
          v._2.index += (t.dstAttr._2.id -> min)
          v._2.hop += (t.dstAttr._2.id)
          t.dstAttr._2.ishop = true //将该hop点标记
          v._2.change = true//加入索引才修改change，否则表示BFS后一圈都不更新，则无需再BFS搜更外面的圈
        }
      })

      if (t.dstAttr._2.ban == false) t.dstAttr._2.que = true //入队
    }
      , _ + _)

    val check : Boolean = g.vertices.map(v => v._2._2.change)reduce((v1, v2) => v1 || v2)
    if (check){
      g.mapVertices((vid, v) => v._2.change = false)
      propagateEdgeCount(g, vk)
    } else {
      g
    }
  }


    val bfsVer: RDD[(VertexId, (String, attribute))] = ver.map(x => Tuple2(x._1, Tuple2(x._2, new attribute(x._1))))
    val newGraph: Graph[(String, attribute), String] = Graph(bfsVer, rdd2)
    bfsVer.foreach(vertexid => {
      val vk = vertexid._1 //遍历到第k个点
      var count = 1 //表示vk是否有其他可拓展点
      newGraph.mapVertices((vid, v) => if(vid == vk) v._2.que = true)
      while (count != 0) {
        count = 0
        val graph1: VertexRDD[Int] = newGraph.aggregateMessages[Int](t => if (t.srcAttr._2.que == true && t.dstAttr._2.que == false && t.dstAttr._2.ban == false) {
          count = 1
          t.sendToDst(1)
          val min: Long = t.srcAttr._2.p + 1
          for (x <- t.dstAttr._2.hop) { //比较Qk-1和当前p
            newGraph.mapVertices((vid,v) => if (vid == vk) {
              if (v._2.hop.contains(x)) { //找出Qk-1中src和dst距离
                if (String.valueOf(v._2.index.get(x)) + t.dstAttr._2.index.get(x) <= String.valueOf(min)) {
                  t.dstAttr._2.ban = true //切掉
                }
              }
            })
          }

          //未被切掉则加入索引集中
          newGraph.mapVertices((vid,v) => if (vid == vk) {
            if (v._2.ban == false){
              v._2.index += (t.dstAttr._2.id -> min)
              v._2.hop += (t.dstAttr._2.id)
              t.dstAttr._2.ishop = true//将该hop点标记
            }
          })

          if (t.dstAttr._2.ban == false) t.dstAttr._2.que = true //入队
        }
          , _ + _)
      }




      //遍历完vk点重置功能属性
      newGraph.mapVertices((vid, v) => {
        v._2.que = false
        v._2.ban = false
        v._2.p = Long.MaxValue
      })

    })
    newGraph.vertices.map


//  }

}
class attribute(vertexId: VertexId){
  var change : Boolean = false//表示修改过用于退出迭代
  val id = vertexId
  var ban : Boolean = false//表示剪切该枝
  val hop :  scala.collection.mutable.Set[VertexId] =  scala.collection.mutable.Set()
  val index : scala.collection.mutable.Map[VertexId, Long] = scala.collection.mutable.Map[VertexId, Long]()
  var que : Boolean = false
  var p = Long.MaxValue
  var ishop : Boolean = false//默认表示不是hop点
  //hop点索引结构
  var xLable : scala.collection.mutable.Set[VertexId] =  scala.collection.mutable.Set()
  var yLable : scala.collection.mutable.Set[VertexId] =  scala.collection.mutable.Set()
}
