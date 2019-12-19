package main
import org.apache.spark.{SparkConf, SparkContext, graphx, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.graphx.impl.GraphImpl

import scala.io.StdIn
import scala.collection.mutable
//import scala.collection.mutable.Set

object Compute {
  def propagateBFS(g: Graph[(String, attribute), String], vk: VertexId): Graph[(String, attribute), String] = {
    g.aggregateMessages[PartitionID](t => if (t.srcAttr._2.que.&&(!t.dstAttr._2.que).&&(!t.dstAttr._2.ban)) {
      t.sendToDst(1)
      val min: VertexId = t.srcAttr._2.p + 1
      for (x <- t.dstAttr._2.hop) { //比较Qk-1和当前p
        g.mapVertices((vid, v) => if (vid == vk) {
          if (v._2.hop.contains(x)) { //找出Qk-1中src和dst距离
            val lo = v._2.index.getOrElse[Long](x, 0) + t.dstAttr._2.index.getOrElse[Long](x, 0)
            if (lo <= min) {
              t.dstAttr._2.ban = true //切掉
            }
          }
        })
      }

      //未被切掉则加入索引集中
      g.mapVertices((vid, v) => if (vid == vk) {
        if (!v._2.ban) {
          v._2.index += (t.dstAttr._2.id -> min)
          v._2.hop += t.dstAttr._2.id
          t.dstAttr._2.isHop = true //将该hop点标记
          v._2.change = true //加入索引才修改change，否则表示BFS后一圈都不更新，则无需再BFS搜更外面的圈
        }
      })

      if (!t.dstAttr._2.ban) t.dstAttr._2.que = true //入队
    }
      , _ + _)

    val check: Boolean = g.vertices.map(v => v._2._2.change).reduce((v1, v2) => v1 || v2)
    if (check) {
      g.mapVertices((vid, v) => v._2.change = false)
      propagateBFS(g, vk)
    } else {
      g
    }
  }

  /**
    * Returns the shortest directed-edge path from src to dst in the graph. If no path exists, returns
    * the empty list.
    */
  def bfs[VD, ED](graph: Graph[VD, ED], src: VertexId, dst: VertexId): Seq[VertexId] = {
    if (src == dst) return List(src)
    // The attribute of each vertex is (dist from src, id of vertex with dist-1)
    var g: Graph[(Int, VertexId), ED] =
      graph.mapVertices((id, _) => (if (id == src) 0 else Int.MaxValue, 0L))

    // Traverse forward from src
    var dstAttr = (Int.MaxValue, 0L)
    while (dstAttr._1 == Int.MaxValue) {
      val msgs = g.aggregateMessages[(Int, VertexId)](
        e => if (e.srcAttr._1 != Int.MaxValue && e.srcAttr._1 + 1 < e.dstAttr._1) {
          e.sendToDst((e.srcAttr._1 + 1, e.srcId))
        },
        (a, b) => if (a._1 < b._1) a else b).cache()

      if (msgs.count == 0) return List.empty

      g = g.ops.joinVertices(msgs) {
        (id, oldAttr, newAttr) =>
          if (newAttr._1 < oldAttr._1) newAttr else oldAttr
      }.cache()

      dstAttr = g.vertices.filter(_._1 == dst).first()._2
    }

    // Traverse backward from dst and collect the path
    var path: List[VertexId] = dstAttr._2 :: dst :: Nil
    while (path.head != src) {
      path = g.vertices.filter(_._1 == path.head).first()._2._2 :: path
    }

    path
  }

  def main(args: Array[String]): Unit = {
    val filename = "src/University0_0.nt"
    val sc = new SparkConf().setAppName("compute").setMaster("local").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val conf = new SparkContext(sc)
    //  readOWL
    val r = conf.textFile(filename).map(_.split("\\s"))
    val vunion = r.map(_ (0)).union(r.map(_ (2))).distinct.zipWithIndex
    var graph: Graph[String, String] = Graph(vunion.map(_.swap), r.map(x => (x(0), (x(1), x(2))))
      .join(vunion).map(x => (x._2._1._2, (x._2._2, x._2._1._1)))
      .join(vunion).map(x => new Edge(x._2._1._1, x._2._2, x._2._1._2)))

    graph.edges.foreach(v => {
      println(v)
    })
    //    val rdd1: RDD[(VertexId, String)] = conf.makeRDD(Seq(
    //      (3L, "student"), (7L, "postdoc"),
    //      (5L, "prof"), (2L, "prof")
    //    ))
    //    val rdd2: RDD[Edge[String]] = conf.makeRDD(Seq(
    //      Edge(3L, 5L, "0"),
    //      Edge(3L, 7L, "0"),
    //      Edge(3L, 2L, "0"),
    //      Edge(7L, 2L, "0"),
    //    ))

    val ver: VertexRDD[String] = graph.vertices

    //  graphx.EdgeContext[(String, attribute), String, Int]


    var bfsVer: RDD[(VertexId, (String, attribute))] = ver.map(x => Tuple2(x._1, Tuple2(x._2, new attribute(x._1))))
    var newGraph: Graph[(String, attribute), String] = Graph(bfsVer, graph.edges)
    //  bfsVer.foreach(vertexid => {
    //    val vk = vertexid._1 //遍历到第k个点
        println("遍历到第 个点" + newGraph.vertices.count())


    var newerGraph: Graph[(VertexId, (String, attribute)), String] = newGraph.mapVertices((vid, v) => {

      println("进入mapvertices")
      if (!v._2.que) {
        v._2.now = true //该点作为第k个点入队,之后变为false
        v._2.que = true
      }
      (vid + 1, v)
    })
    newerGraph.vertices.count()

    newGraph.vertices.map(v => {
      println("进入mapvertices222")
      if (!v._2._2.que) {
        v._2._2.now = true //该点作为第k个点入队,之后变为false
        v._2._2.que = true
      }
      ( v)
    }).reduce((v1,v2) => v1)

    //    propagateBFS(newGraph, vk)

    var count = 1 //表示vk是否有其他可拓展点
    while (count != 0) {
      count = 0
      println("进入while")
      val graph1: VertexRDD[Long] = newerGraph.aggregateMessages[Long](t => if (t.srcAttr._2._2.que.&&(!t.dstAttr._2._2.que).&&(!t.dstAttr._2._2.ban)) {
        println("进入while-if")
        count = 1
        val min: Long = t.srcAttr._2._2.p + 1
        t.sendToDst(min)
        //        for (x <- t.dstAttr._2._2.hop) { //比较Qk-1和当前p
        //          newGraph.mapVertices((vid,v) => if (vid == vk) {
        //            if (v._2.hop.contains(x)) { //找出Qk-1中src和dst距离
        //              if (String.valueOf(v._2.index.get(x)) + t.dstAttr._2._2.index.get(x) <= String.valueOf(min)) {
        //                t.dstAttr._2._2.ban = true //切掉
        //              }
        //            }
        //          })
        //        }


        //        //未被切掉则加入索引集中
        //        newGraph.mapVertices((vid,v) => if (vid == vk) {
        //          if (!v._2.ban){
        //            v._2.index += (t.dstAttr._2._2.id -> min)
        //            v._2.hop += t.dstAttr._2._2.id
        //            t.dstAttr._2._2.isHop = true//将该hop点标记
        //          }
        //        })

        //        if (!t.dstAttr._2._2.ban) t.dstAttr._2._2.que = true //入队
      } else {
        t.sendToDst(Long.MaxValue)
        t.sendToSrc(Long.MaxValue)
      }
        , (a, b) => if (a < b) a else b)
      //id String attribute long
      val graph1v: RDD[(VertexId, (String, attribute, Long))] = graph1.join(newerGraph.vertices).map(v => (v._1, (v._2._2._2._1, v._2._2._2._2, v._2._1)))
      val newGraph1: Graph[(String, attribute, Long), String] = Graph(graph1v, graph.edges)


    }



    //遍历完vk点重置功能属性
    newGraph.mapVertices((vid, v) => {
      v._2.que = false
      v._2.ban = false
      v._2.p = Long.MaxValue
    })
    //  })


    //对hop点生成索引
    newGraph.mapVertices((vid, v1) => {
      if (v1._2.isHop) {
        newGraph.vertices.map(v2 => Tuple3(v2._1, v2._2._2.hop, v2._2._1)).reduce((v3, v4) => {
          val interestSet = v3._2.intersect(v1._2.hop)
          if (interestSet.nonEmpty) { //表示可达该hop点
            var nameType: String = new String()
            if (v3._3.contains("@")) {
              val array: Array[String] = v3._3.split("@")
              nameType = array(0)
            } else {
              nameType = v3._3
            }
            val allName = Set("University", "Department", "FullProfessor", "AssociateProfessor", "AssistantProfessor",
              "Lecturer", "UndergraduateStudent", "GraduateStudent", "TeachingAssistant", "ResearchAssistant",
              "Course", "GraduateCourse", "Publication", "ResearchGroup")
            for (name <- allName) {
              if (nameType.contains(name)) {
                if (v1._2.reachLabel.contains(name)) {
                  v1._2.reachLabel.getOrElse(name, scala.collection.mutable.Set()) += v3._1
                }
                else {
                  val tempSet: mutable.Set[VertexId] = scala.collection.mutable.Set()
                  tempSet += v3._1
                  v1._2.reachLabel += (name -> tempSet)
                }
              }
            }
          }
          v3
        })
      }
    })
    println("hello ")
    newGraph.vertices.foreach(v => {
      println(v)
      println(v._2._2.hop.size)
    })
    //  var cou = 3
    while (true) {
      println("请输入：1最短路径查询 2可达性查询 3图匹配")
      //    cou = cou -1
      val n: Int = StdIn.readInt()
      n match {
        case 1 =>
          println("请输入起点名")
          val start: String = StdIn.readLine()
          println("请输入终点名")
          val end: String = StdIn.readLine()
          newGraph.vertices.foreach(v => {
            println("进入1")
            if (v._2._1 == start) {
              println("进入1  if")
              newGraph.vertices.foreach(v2 => {
                println("进入2")
                if (v2._2._1 == end) {
                  val interestSet = v._2._2.hop.intersect(v2._2._2.hop)
                  var min = Long.MaxValue
                  if (interestSet.nonEmpty) {
                    for (x <- interestSet) {
                      val dis: Long = v._2._2.index.getOrElse[Long](x, 0) + v2._2._2.index.getOrElse[Long](x, 0)
                      if (dis < min)
                        min = dis
                    }
                  }
                  if (min == Long.MaxValue) {
                    println("最短距离为 " + min)
                  } else {
                    println("最短距离不存在")
                  }
                }
              })
            }
          })

        case 2 =>
          println("请输入起点名")
          val start: String = StdIn.readLine()
          println("请输入终点名")
          val end: String = StdIn.readLine()
          newGraph.vertices.foreach(v => {
            if (v._2._1 == start)
              newGraph.vertices.foreach(v2 => {
                if (v2._2._1 == end) {
                  val interestSet = v._2._2.hop.intersect(v2._2._2.hop)
                  if (interestSet.nonEmpty) {
                    println("可达！")
                  } else {
                    println("不可达！")
                  }
                }
              })
          })

        case 3 =>


      }


    }
  }
}
class attribute(vertexId: VertexId){
  var change : Boolean = false//表示修改过用于退出迭代
  val id : VertexId = vertexId
  var ban : Boolean = false//表示剪切该枝
  val hop :  scala.collection.mutable.Set[VertexId] =  scala.collection.mutable.Set()
  val index : scala.collection.mutable.Map[VertexId, Long] = scala.collection.mutable.Map[VertexId, Long]()
  var que : Boolean = false
  var p : Long = Long.MaxValue
  var isHop : Boolean = false//默认表示不是hop点
  //hop点索引结构
  var reachLabel : scala.collection.mutable.Map[String, scala.collection.mutable.Set[VertexId]] = scala.collection.mutable.Map[String, scala.collection.mutable.Set[VertexId]]()
//  var yLabel : scala.collection.mutable.Set[VertexId] =  scala.collection.mutable.Set()
  var now : Boolean = false
}
