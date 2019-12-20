package main
import org.apache.spark.{SparkConf, SparkContext, graphx, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.graphx.impl.GraphImpl
import scala.util.control._
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
    conf.setCheckpointDir("/src/checkpoint")
    //  readOWL
    val r = conf.textFile(filename).map(_.split("\\s"))
    val vunion = r.map(_ (0)).union(r.map(_ (2))).distinct.zipWithIndex
//    var graph: Graph[String, String] = Graph(vunion.map(_.swap), r.map(x => (x(0), (x(1), x(2))))
//      .join(vunion).map(x => (x._2._1._2, (x._2._2, x._2._1._1)))
//      .join(vunion).map(x => new Edge(x._2._1._1, x._2._2, x._2._1._2)))

//    graph.edges.foreach(v => {
//      println(v)
//    })
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
    var graph: Graph[String, String] = Graph(rdd1, rdd2)

    val ver: VertexRDD[String] = graph.vertices

    var bfsVer: RDD[(VertexId, (String, attribute))] = ver.map(x => Tuple2(x._1, Tuple2(x._2, new attribute(x._1))))
    var newGraph: Graph[(String, attribute), String] = Graph(bfsVer, graph.edges)
    //  bfsVer.foreach(vertexid => {
    //    val vk = vertexid._1 //遍历到第k个点
//        println("遍历到第 个点" + newGraph.vertices.count())



    var newerGraph: Graph[(String, attribute), String] = newGraph.mapVertices((vid, v) => {
      println("进入mapvertices")
      v._2.hop += vid//自己到自己有打标记距离为0，但不作为hop点，可有效减少hop点的数据量
      v._2.index += (vid -> 0)
      v
    })
    newerGraph.vertices.count()

    for (i <- 1L to newerGraph.vertices.count()) {
      var number = 0
      newerGraph = newerGraph.mapVertices((vid, v) => {
        println("进入mapvertices")
        if ((!v._2.done).&&(number == 0)) {
          v._2.now = true //该点作为第k个点入队,之后变为false
          v._2.que = true
          v._2.p = 0 //BFS出发点距离置为0
          number = 1
        }
        v
      })
      //蜜汁需要更新一下，否则在mapVertex中edges的src和dst的属性不会与vertex保持同步
      newerGraph = Graph(newerGraph.vertices, graph.edges)
      newerGraph.checkpoint()
      newerGraph.vertices.count()
//      println("for初始number情况 " + number)
      newerGraph.vertices.foreach(v => {
        println("for初始v情况 id "+ v._1 +" que " + v._2._2.que + " now" + v._2._2.now + " ban" + v._2._2.ban)
      })
      val test: VertexRDD[VertexId] = newerGraph.aggregateMessages[VertexId](t => {
        t.sendToSrc(1)
        println("newerGraph属性情况：")
        print(t.srcId)
        println(t.srcAttr._2.que)
        print(t.dstId)
        println(t.dstAttr._2.que)
        println(t.dstAttr._2.ban)
      }, (a, b) => if (a < b) a else b)
      test.count()


      //获得当前出发点的index和hop集合
      val graph1Vertex: VertexRDD[(String, attribute)] = newerGraph.vertices
      val setRdd: RDD[(VertexId, Boolean, mutable.Set[VertexId], mutable.Map[VertexId, Long])] = graph1Vertex.map(v => (v._1, v._2._2.now, v._2._2.hop, v._2._2.index))
      val nowTuple: (VertexId, Boolean, mutable.Set[VertexId], mutable.Map[VertexId, Long]) = setRdd.reduce((v1, v2) => {
        if (v1._2)
          v1
        else if (v2._2)
          v2
        else
          v1
      })
      val nowSet: mutable.Set[VertexId] = nowTuple._3
      val nowIndex: mutable.Map[VertexId, Long] = nowTuple._4
      val nowID: VertexId = nowTuple._1
      var nowisHop: Boolean = false


//      var count = 1
      var graph1: RDD[(VertexId, Long)] = conf.makeRDD(Seq(
        (3L, 1L)
      ))
      var count = graph1.map(v=>v._2).reduce((a,b) => {
        if (a < b){
          a
        } else {
          b
        }
      })
      var regre = 0
      //表示vk是否有其他可拓展点
      //这里不可用 var count = 0, 进入if count = 1 判断，因为count在一个rdd操作修改后回到本地的值
      while (count != Long.MaxValue) {
        regre =  regre + 1
        println("进入while")
        graph1 = newerGraph.aggregateMessages[Long](t => {if (t.srcAttr._2.que.&&(!t.dstAttr._2.que).&&(!t.dstAttr._2.ban)) {
          println("进入while-if")
          val min: Long = t.srcAttr._2.p + 1
          t.sendToDst(min)
          t.sendToSrc(Long.MaxValue)
          //延迟入队
        } else if(t.dstAttr._2.que.&&(!t.srcAttr._2.que).&&(!t.srcAttr._2.ban)){//此处作为无向图，故需判断两点分别作为起点的情况
          println("进入while-if")
          val min: Long = t.dstAttr._2.p + 1
          t.sendToSrc(min)
          t.sendToDst(Long.MaxValue)
          //延迟入队
        } else {
            t.sendToDst(Long.MaxValue)
            t.sendToSrc(Long.MaxValue)
          }
        }
          , (a, b) => if (a < b) a else b)
//        println("出aggregateMessages后的count " + count)
        graph1.count()
        graph1.foreach(v => {
          println("graph1情况 id" + v._1 + " min" + v._2)
        })
        //id String attribute long <- id long String attribute
        val graph1v: RDD[(VertexId, (String, attribute, Long))] = graph1.join(newerGraph.vertices).map(v => (v._1, (v._2._2._1, v._2._2._2, v._2._1)))
        graph1v.count()
        graph1v.foreach(v => {
          println("graph1v情况 id" + v._1 +" que" + v._2._2.que + " now" + v._2._2.now + " min" + v._2._3)
        })


        val newGraph1: Graph[(String, attribute, Long), String] = Graph(graph1v, graph.edges)
        newGraph1.vertices.foreach(v => {
          println("for初始v情况 que" + v._2._2.que + " now" + v._2._2.now + " min" + v._2._3)
        })


        count = graph1.map(v=>v._2).reduce((a,b) => {
          if (a < b){
            a
          } else {
            b
          }
        })
        if (count != Long.MaxValue) { //表示有边更新则进行索引计算
          println("进入if count" )
          val newGraph2Vertex: VertexRDD[Boolean] = newGraph1.aggregateMessages[Boolean](t => if (t.srcAttr._2.que.&&(!t.dstAttr._2.que).&&(!t.dstAttr._2.ban)) {
            for (x <- t.dstAttr._2.hop) { //比较Qk-1和当前p
              if (nowSet.contains(x)) { //找出Qk-1中src和dst距离
                if (nowIndex.getOrElse[Long](x, 0) + t.dstAttr._2.index.getOrElse[Long](x, 0) <= t.dstAttr._3) {
                  t.dstAttr._2.ban = true //切掉
                }
              }
            }
            t.sendToDst(t.dstAttr._2.ban)
//            t.sendToSrc(false)

            //注意！！！！在Aggregate中不会更新调用图
//            //未被切掉则更新目标顶点索引集
//            if (!t.dstAttr._2.ban) {
//              t.dstAttr._2.index += (nowID -> t.dstAttr._3)
//              t.dstAttr._2.hop += nowID
//              nowisHop = true //将出发点作为hop点标记
//              t.dstAttr._2.que = true //入队
//              t.dstAttr._2.p = t.dstAttr._3 //入队
//              //显示未被切掉点的修改情况
//              println("未被切掉点的修改情况 id" + t.dstId +" que"+ t.dstAttr._2.que)
//            }
            } else if(t.dstAttr._2.que.&&(!t.srcAttr._2.que).&&(!t.srcAttr._2.ban)){//同理无向图中需检测双点
            for (x <- t.srcAttr._2.hop) { //比较Qk-1和当前p
              if (nowSet.contains(x)) { //找出Qk-1中src和dst距离
                if (nowIndex.getOrElse[Long](x, 0) + t.srcAttr._2.index.getOrElse[Long](x, 0) <= t.srcAttr._3) {
                  t.srcAttr._2.ban = true //切掉,!!注意此时ban并未在实际的RDD中修改
                }
              }
            }
            t.sendToSrc(t.srcAttr._2.ban)
          }
            //不给其他点发
//          } else {
//            t.sendToDst(false)
//            t.sendToSrc(false)
//          }
            , (a, b) => {
              if (a){
                a
              } else {
                b
              }
            })
          newGraph2Vertex.count()

          val graph2JoinVertex: RDD[(VertexId, (Boolean, (String, attribute, Long)))] = newGraph2Vertex.join(newGraph1.vertices)
          val graph2JoinUpdate: RDD[(VertexId, (Boolean, (String, attribute, Long)))] = graph2JoinVertex.map(v => {
            if (!v._2._1) {
              v._2._2._2.index += (nowID -> v._2._2._3)
              v._2._2._2.hop += nowID
              nowisHop = true //将出发点作为hop点标记
              v._2._2._2.que = true //入队
              v._2._2._2.p = v._2._2._3 //入队
              //显示未被切掉点的修改情况
              println("未被切掉点的修改情况 id" + v._1 + " que" + v._2._2._2.que)
            } else {
              v._2._2._2.ban = true//ban置为true，下次不访问它
            }
            v
          })
          val newGraph2JoinBan: RDD[(VertexId, ((String, attribute, Long), Option[(Boolean, (String, attribute, Long))]))] = newGraph1.vertices.leftOuterJoin(graph2JoinUpdate)
          //显示newGraph2JoinBan中点的修改情况
          newGraph2JoinBan.foreach(v => {
            val tupleBan: (Boolean, (String, attribute, VertexId)) = v._2._2.getOrElse(null)

            println("newGraph2JoinBan中点的修改情况 id" + v._1 + tupleBan)
          })
          val formatGraph2Vertex: RDD[(VertexId, (String, attribute))] = newGraph2JoinBan.map(v => {
            val tupleBan: (Boolean, (String, attribute, VertexId)) = v._2._2.getOrElse(null)
            if (tupleBan == null) {
              (v._1, (v._2._1._1, v._2._1._2))
            } else {
              (v._1, (tupleBan._2._1, tupleBan._2._2))
            }
          })
          formatGraph2Vertex.count()

          //显示newGraph1中点的修改情况
          formatGraph2Vertex.foreach(v => {
            println("newGraph1中点的修改情况 id" + v._1 +" que"+ v._2._2.que + " now" + v._2._2.now + " hop" + v._2._2.hop)
          })

//          //转换newGraph2Vertex格式到newerGraph格式，修正到newerGraph中
//          val newGraph1Vertex: VertexRDD[(String, attribute, Long)] = newGraph1.vertices
//          val fitFormRdd: RDD[(VertexId, (String, attribute))] = newGraph1Vertex.map(v => (v._1, (v._2._1, v._2._2)))
//          fitFormRdd.count()
          newerGraph = Graph(formatGraph2Vertex, graph.edges)

          //显示newerGraph中点的修改情况
          newerGraph.vertices.foreach(v => {
            println("newerGraph中点的修改情况 id" + v._1 +" que"+ v._2._2.que + " now" + v._2._2.now + " hop" + v._2._2.hop)
          })
        }
      }

      println("while 迭代次数 " + regre)

      //遍历完vk点重置功能属性
      newerGraph = newerGraph.mapVertices((vid, v) => {
        println("进入mapvertices")
        if (v._2.now) {
          v._2.now = false //该点作为第k个点BFS完成后标记
          v._2.hop = nowSet //更新出发点的结构
          v._2.index = nowIndex
          v._2.isHop = nowisHop
          v._2.done = true
        }
        v._2.que = false
        v._2.ban = false
        v._2.p = Long.MaxValue
        v
      })


      newerGraph.vertices.count()
    }



    //对hop点生成索引
    for (i <- 1L to newerGraph.vertices.count()) {
      var number = 0
      newerGraph = newerGraph.mapVertices((vid, v) => {
        println("进入mapvertices")
        if ((!v._2.doneHop).&&(v._2.isHop) .&&(number == 0)) {
          v._2.nowHop = true //该点作为当前遍历的Hop点
          number = 1
        }
        v
      })
      newerGraph.checkpoint()
      newerGraph.vertices.count()

      //获得当前遍历到的hop点的index和hop集合
      val graphHopVertex: VertexRDD[(String, attribute)] = newerGraph.vertices
      val setHopRdd: RDD[(VertexId, Boolean, mutable.Set[VertexId], mutable.Map[String, scala.collection.mutable.Set[VertexId]])] = graphHopVertex.map(v => (v._1, v._2._2.nowHop, v._2._2.hop, v._2._2.reachLabel))
      val hopTuple: (VertexId, Boolean, mutable.Set[VertexId], mutable.Map[String, scala.collection.mutable.Set[VertexId]]) = setHopRdd.reduce((v1, v2) => {
        if (v1._2)
          v1
        else if (v2._2)
          v2
        else
          v1
      })
      val hopSet: mutable.Set[VertexId] = hopTuple._3
      val hopReach: mutable.Map[String, scala.collection.mutable.Set[VertexId]] = hopTuple._4


      val hopNewerGraphVertex: RDD[(VertexId, mutable.Set[VertexId], String)] = newerGraph.vertices.map(v2 => Tuple3(v2._1, v2._2._2.hop, v2._2._1))
      hopNewerGraphVertex.map(v3 => {
        val interestSet = v3._2.intersect(hopSet)
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
              if (hopReach.contains(name)) {
                hopReach.getOrElse(name, scala.collection.mutable.Set()) += v3._1
              }
              else {
                val tempSet: mutable.Set[VertexId] = scala.collection.mutable.Set()
                tempSet += v3._1
                hopReach += (name -> tempSet)
              }
            }
          }
        }
        v3
      })
      hopNewerGraphVertex.count()

      //设置完当前hop点后更新功能属性
      newerGraph = newerGraph.mapVertices((vid, v) => {
        println("进入mapvertices")
        if (v._2.nowHop) {
          v._2.nowHop = false //该点作为nowHop更新后标记
          v._2.reachLabel = hopReach //更新nowHop点的结构
          v._2.doneHop = true
        }
        v
      })
      newerGraph.checkpoint()
      newerGraph.vertices.count()
    }



    //对hop点生成索引
//    newGraph.mapVertices((vid, v1) => {
//      if (v1._2.isHop) {
//        newGraph.vertices.map(v2 => Tuple3(v2._1, v2._2._2.hop, v2._2._1)).reduce((v3, v4) => {
//          val interestSet = v3._2.intersect(v1._2.hop)
//          if (interestSet.nonEmpty) { //表示可达该hop点
//            var nameType: String = new String()
//            if (v3._3.contains("@")) {
//              val array: Array[String] = v3._3.split("@")
//              nameType = array(0)
//            } else {
//              nameType = v3._3
//            }
//            val allName = Set("University", "Department", "FullProfessor", "AssociateProfessor", "AssistantProfessor",
//              "Lecturer", "UndergraduateStudent", "GraduateStudent", "TeachingAssistant", "ResearchAssistant",
//              "Course", "GraduateCourse", "Publication", "ResearchGroup")
//            for (name <- allName) {
//              if (nameType.contains(name)) {
//                if (v1._2.reachLabel.contains(name)) {
//                  v1._2.reachLabel.getOrElse(name, scala.collection.mutable.Set()) += v3._1
//                }
//                else {
//                  val tempSet: mutable.Set[VertexId] = scala.collection.mutable.Set()
//                  tempSet += v3._1
//                  v1._2.reachLabel += (name -> tempSet)
//                }
//              }
//            }
//          }
//          v3
//        })
//      }
//    })
    println("hello ")
    newerGraph.vertices.foreach(v => {
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


          //获得当前输入的起点的index和hop集合
          val graphHopVertex: VertexRDD[(String, attribute)] = newerGraph.vertices
          val setHopRdd: RDD[(VertexId, Boolean, mutable.Set[VertexId], mutable.Map[String, scala.collection.mutable.Set[VertexId]])] = graphHopVertex.map(v => (v._1, v._2._2.nowHop, v._2._2.hop, v._2._2.reachLabel))
          val hopTuple: (VertexId, Boolean, mutable.Set[VertexId], mutable.Map[String, scala.collection.mutable.Set[VertexId]]) = setHopRdd.reduce((v1, v2) => {
            if (v1._2)
              v1
            else if (v2._2)
              v2
            else
              v1
          })
          val hopSet: mutable.Set[VertexId] = hopTuple._3
          val hopReach: mutable.Map[String, scala.collection.mutable.Set[VertexId]] = hopTuple._4
          val hopID: VertexId = hopTuple._1


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
  var doneHop : Boolean = false//表示是否作为生成hop索引时的起始点遍历过
  var nowHop : Boolean = false//表示是否为当前正在遍历的hop点
  var done : Boolean = false//表示是否作为起始点遍历过
  var change : Boolean = false//表示修改过用于退出迭代
  val id : VertexId = vertexId
  var ban : Boolean = false//表示剪切该枝
  var hop :  scala.collection.mutable.Set[VertexId] =  scala.collection.mutable.Set()
  var index : scala.collection.mutable.Map[VertexId, Long] = scala.collection.mutable.Map[VertexId, Long]()
  var que : Boolean = false
  var p : Long = Long.MaxValue
  var isHop : Boolean = false//默认表示不是hop点
  //hop点索引结构
  var reachLabel : scala.collection.mutable.Map[String, scala.collection.mutable.Set[VertexId]] = scala.collection.mutable.Map[String, scala.collection.mutable.Set[VertexId]]()
//  var yLabel : scala.collection.mutable.Set[VertexId] =  scala.collection.mutable.Set()
  var now : Boolean = false
}
