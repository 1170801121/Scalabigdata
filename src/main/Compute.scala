package main
import org.apache.spark.{SparkConf, SparkContext, graphx, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.graphx.impl.GraphImpl

import scala.util.control._
import scala.io.StdIn
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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
          (5L, "prof"), (2L, "profed")
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
      //需要更新是因为edge是地址添加，而非值添加
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
      newerGraph = Graph(newerGraph.vertices, graph.edges)
    }


    //所有点的类型
    val allName = Set("University", "Department", "FullProfessor", "AssociateProfessor", "AssistantProfessor",
      "Lecturer", "UndergraduateStudent", "GraduateStudent", "TeachingAssistant", "ResearchAssistant",
      "Course", "GraduateCourse", "Publication", "ResearchGroup")
    //对hop点生成索引，加上可达到hop点的点到reach中
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
      //蜜汁需要更新一下，否则在mapVertex中edges的src和dst的属性不会与vertex保持同步
      newerGraph = Graph(newerGraph.vertices, graph.edges)
      newerGraph.checkpoint()
      newerGraph.vertices.count()

      //获得当前遍历到的hop点的index和hop集合
      val graphHopVertex: VertexRDD[(String, attribute)] = newerGraph.vertices
      val setHopRdd: RDD[(VertexId, Boolean, mutable.Set[VertexId], mutable.Map[String, scala.collection.mutable.Set[String]], String)] = graphHopVertex.map(v => (v._1, v._2._2.nowHop, v._2._2.hop, v._2._2.reachLabel, v._2._1))
      val hopTuple: (VertexId, Boolean, mutable.Set[VertexId], mutable.Map[String, scala.collection.mutable.Set[String]], String) = setHopRdd.reduce((v1, v2) => {
        if (v1._2)
          v1
        else if (v2._2)
          v2
        else
          v1
      })
      val hopSet: mutable.Set[VertexId] = hopTuple._3
      val hopReach: mutable.Map[String, scala.collection.mutable.Set[String]] = hopTuple._4
//      val hopName: String = hopTuple._5
//      hopReach += (hopName -> tempSet)

      val hopNewerGraphVertex: RDD[(VertexId, mutable.Set[VertexId], String)] = newerGraph.vertices.map(v2 => Tuple3(v2._1, v2._2._2.hop, v2._2._1))
      val hopUpdateVertex: RDD[(VertexId, mutable.Set[VertexId], String)] = hopNewerGraphVertex.map(v3 => {
        val interestSet = v3._2.intersect(hopSet)
        if (interestSet.nonEmpty) { //表示可达该hop点
          var nameType: String = new String()
          if (v3._3.contains("@")) {
            val array: Array[String] = v3._3.split("@")
            nameType = array(0)
          } else {
            nameType = v3._3
          }
          //将该可达点按类型记录到hopReach中
          for (name <- allName) {
            if (nameType.contains(name)) {
              if (hopReach.contains(name)) {
                hopReach.getOrElse(name, scala.collection.mutable.Set()) += v3._3
              }
              else {
                val tempSet: mutable.Set[String] = scala.collection.mutable.Set()
                tempSet += v3._3
                hopReach += (name -> tempSet)
              }
            }
          }
        }
        v3
      })
      hopUpdateVertex.count()

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
      newerGraph = Graph(newerGraph.vertices, graph.edges)
    }

    //建立W表
    var tableW : scala.collection.mutable.Map[(String, String), scala.collection.mutable.Set[String]] = scala.collection.mutable.Map[(String, String), scala.collection.mutable.Set[String]]()
    val allNameList: List[String] = allName.toList
    val tupleList: Iterator[List[String]] = allNameList.sorted.combinations(2)
    tupleList.foreach({ L =>
      val tableWTuple: (String, String) = Tuple2(L.apply(0), L.apply(1))
//      if (tableW.contains(tableWTuple)) {
//      } else {
      val tempSet: mutable.Set[String] = scala.collection.mutable.Set()
      tableW += (tableWTuple -> tempSet)
//      }
    })
    //id isHop reachlabel string
    val tableWVertex: RDD[(VertexId, Boolean,  mutable.Map[String, scala.collection.mutable.Set[String]], String)] = newerGraph.vertices.map(v2 => Tuple4(v2._1,v2._2._2.isHop , v2._2._2.reachLabel, v2._2._1))
    val updateTableW = tableWVertex.map(v => {
      if (v._2) {
        for ((xType, yType) <- tableW.keySet) {
          //可满足xy类型则加入W表
          if (v._3.contains(xType).&&(v._3.contains(yType))) {
            tableW.getOrElse((xType, yType), scala.collection.mutable.Set()) += v._4
          }
        }
      }
      v
    })
    updateTableW.count()



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
      println(v._2._2.reachLabel)
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
          val startVertex: VertexRDD[(String, attribute)] = newerGraph.vertices
          val startVertexForm: RDD[(VertexId, String, mutable.Set[VertexId], mutable.Map[VertexId, Long])] = startVertex.map(v => (v._1, v._2._1, v._2._2.hop, v._2._2.index))
          val startTuple: (VertexId, String, mutable.Set[VertexId], mutable.Map[VertexId, Long]) = startVertexForm.reduce((v1, v2) => {
            if (v1._2 == start)
              v1
            else
              v2
          })
          val startSet: mutable.Set[VertexId] = startTuple._3
          val startIndex: mutable.Map[VertexId, Long] = startTuple._4

          //获得当前输入的终点的index和hop集合
          val endTuple: (VertexId, String, mutable.Set[VertexId], mutable.Map[VertexId, Long]) = startVertexForm.reduce((v1, v2) => {
            if (v1._2 == end)
              v1
            else
              v2
          })
          val endSet: mutable.Set[VertexId] = endTuple._3
          val endIndex: mutable.Map[VertexId, Long] = endTuple._4
          val interestSet = startSet.intersect(endSet)
          var min = Long.MaxValue
          if (interestSet.nonEmpty) {
            for (x <- interestSet) {
              val dis: Long = startIndex.getOrElse[Long](x, 0) + endIndex.getOrElse[Long](x, 0)
              if (dis < min)
                min = dis
            }
          }
          if (min != Long.MaxValue) {
            println("最短距离为 " + min)
          } else {
            println("最短距离不存在")
          }

        case 2 =>
          println("请输入起点名")
          val start: String = StdIn.readLine()
          println("请输入终点名")
          val end: String = StdIn.readLine()


          //获得当前输入的起点的index和hop集合
          val startVertex: VertexRDD[(String, attribute)] = newerGraph.vertices
          val startVertexForm: RDD[(VertexId, String, mutable.Set[VertexId], mutable.Map[VertexId, Long])] = startVertex.map(v => (v._1, v._2._1, v._2._2.hop, v._2._2.index))
          val startTuple: (VertexId, String, mutable.Set[VertexId], mutable.Map[VertexId, Long]) = startVertexForm.reduce((v1, v2) => {
            if (v1._2 == start)
              v1
            else
              v2
          })
          val startSet: mutable.Set[VertexId] = startTuple._3


          //获得当前输入的终点的index和hop集合
          val endTuple: (VertexId, String, mutable.Set[VertexId], mutable.Map[VertexId, Long]) = startVertexForm.reduce((v1, v2) => {
            if (v1._2 == end)
              v1
            else
              v2
          })
          val endSet: mutable.Set[VertexId] = endTuple._3
          val interestSet = startSet.intersect(endSet)
          if (interestSet.nonEmpty) {
            println("可达！！ ")
          }
          else {
            println("不可达！！")
          }

        case 3 =>
          println("请输入要匹配的图模式，以空格隔开,输入0终止,后一个为新边")
          var start: String = StdIn.readLine()
          var startNameType: String = new String()
          var endNameType: String = new String()
          // 创建 Breaks 对象
          val loop = new Breaks
          while (true) {
            if (start.contains("\\s")) {
              val array: Array[String] = start.split("\\s")
              startNameType = array(0)
              endNameType = array(1)
              loop.break()
            } else {
              println("输入不符合规则,重新输入")
              start = StdIn.readLine()
            }
          }
          var allListToSet:  scala.collection.mutable.Set[ListBuffer[String]] = scala.collection.mutable.Set[ListBuffer[String]]()
          var allListToBeDown:  scala.collection.mutable.Set[ListBuffer[String]] = scala.collection.mutable.Set[ListBuffer[String]]()//在filter中要被过滤掉的list的集合
          var nameList : scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()
//          开头存点的类型，各list一一对应，存每个类型的可匹配模式的点
//          var list1 : scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()
//          var list2 : scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()
//          allListToSet += list1
//          allListToSet += list2
//          list1.append(startNameType)
//          list2.append(endNameType)
          val hopSatisfyVertex: mutable.Set[String] = tableW.getOrElse((startNameType, endNameType), scala.collection.mutable.Set())
          if (hopSatisfyVertex.nonEmpty){
            for (hopVertex <- hopSatisfyVertex){
              //获得当前输入的点的reachLabel集合
              val startVertex: VertexRDD[(String, attribute)] = newerGraph.vertices
              val startVertexForm: RDD[(VertexId, String, mutable.Map[String, scala.collection.mutable.Set[String]])] = startVertex.map(v => (v._1, v._2._1, v._2._2.reachLabel))
              val startTuple: (VertexId, String, mutable.Map[String, scala.collection.mutable.Set[String]]) = startVertexForm.reduce((v1, v2) => {
                if (v1._2 == hopVertex)
                  v1
                else
                  v2
              })
              val hopReachLabel: mutable.Map[String, scala.collection.mutable.Set[String]] = startTuple._3
              val startHopVertex: mutable.Set[String] = hopReachLabel.getOrElse(startNameType, scala.collection.mutable.Set())
              val endHopVertex: mutable.Set[String] = hopReachLabel.getOrElse(endNameType, scala.collection.mutable.Set())
//              for (shopName <- startHopVertex){
//                list1.append(shopName)
//              }
//              for (ehopName <- endHopVertex){
//                list2.append(ehopName)
//              }

              //将符合模式的点对加入,同时nameList更新
              for (startName1 <- startHopVertex){
                for (endName1 <- endHopVertex){
                  var list1 : scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()
                  list1.append(startName1)
                  list1.append(endName1)
                  allListToSet += list1
                }
              }
              nameList.append(startNameType)
              nameList.append(endNameType)
            }
          }
          start = StdIn.readLine()
          while (start != 0) {
            var newStartNameType: String = new String()
            var newEndNameType: String = new String()
            while (true) {
              if (start.contains("\\s")) {
                val array: Array[String] = start.split("\\s")
                newStartNameType = array(0)
                newEndNameType = array(1)
                loop.break()
              } else {
                println("输入不符合规则,重新输入")
                start = StdIn.readLine()
              }
            }
            val tableWHopFind: mutable.Set[String] = tableW.getOrElse((newStartNameType, newEndNameType), scala.collection.mutable.Set())
//            val deleteLoc : scala.collection.mutable.ListBuffer[Int] = scala.collection.mutable.ListBuffer[Int]()//记录待删除的位置
            //先执行filter操作，找到输入的nametype中对应之前结果的list，对该list进行过滤
            for (nowlist <- allListToSet){
              var loc = 0
              for (i <- 0 to nameList.size){
                if (nameList.apply(i) == newStartNameType){
                  loc = i
                }
              }
              //获得当前list中loc位置的点的hop集合
              val listVertex: VertexRDD[(String, attribute)] = newerGraph.vertices
              val listVertexForm: RDD[(VertexId, String, mutable.Set[VertexId])] = listVertex.map(v => (v._1, v._2._1, v._2._2.hop))
              val listTuple: (VertexId, String, mutable.Set[VertexId]) = listVertexForm.reduce((v1, v2) => {
                if (v1._2 == nowlist.apply(loc))
                  v1
                else
                  v2
              })
              val listHop: mutable.Set[VertexId] = listTuple._3
              var idict : Boolean = false//表示是否可以满足需要的模式
              for (listHopID <- listHop) {
                //获得hop点的string，到W表中查询是否满足模式
                val startVertex: VertexRDD[(String, attribute)] = newerGraph.vertices
                val startVertexForm: RDD[(VertexId, String)] = startVertex.map(v => (v._1, v._2._1))
                val startTuple: (VertexId, String) = startVertexForm.reduce((v1, v2) => {
                  if (v1._1 == listHopID)
                    v1
                  else
                    v2
                })
                val listHopString: String = startTuple._2
                if (tableWHopFind.contains(listHopString)){
                  idict = true
                }
              }
              if (!idict){
                allListToBeDown += nowlist
              }
//              if (nowlist.apply(0) == newStartNameType){
//                //先找到list中点可达的hop点，然后判断这些hop点是否满足模式，不满足则filter，这里因为是无向图，所以可达hop点就是之前生成的hop集合
//                for (i <- 1 to (nowlist.size - 1)) {
//                  //获得当前list中的点的hop集合
//                  val listVertex: VertexRDD[(String, attribute)] = newerGraph.vertices
//                  val listVertexForm: RDD[(VertexId, String, mutable.Set[VertexId])] = listVertex.map(v => (v._1, v._2._1, v._2._2.hop))
//                  val listTuple: (VertexId, String, mutable.Set[VertexId]) = listVertexForm.reduce((v1, v2) => {
//                    if (v1._2 == nowlist.apply(i))
//                      v1
//                    else
//                      v2
//                  })
//                  val listHop: mutable.Set[VertexId] = listTuple._3
//                  var idict : Boolean = false
//                  for (listHopID <- listHop) {
//                    //获得hop点的string，到W表中查询是否满足模式
//                    val startVertex: VertexRDD[(String, attribute)] = newerGraph.vertices
//                    val startVertexForm: RDD[(VertexId, String)] = startVertex.map(v => (v._1, v._2._1))
//                    val startTuple: (VertexId, String) = startVertexForm.reduce((v1, v2) => {
//                      if (v1._1 == listHopID)
//                        v1
//                      else
//                        v2
//                    })
//                    val listHopString: String = startTuple._2
//                    if (tableWHopFind.contains(listHopString)){
//                      idict = true
//                    }
//                  }
//                  if (!idict){
//                    allListToBeDown += nowlist
//                  }
//                }
//              }
            }
            allListToSet = allListToSet -- allListToBeDown
            allListToBeDown = scala.collection.mutable.Set[ListBuffer[String]]()

            //fetch操作
            var listNewInput : scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer[String]()
            allListToSet += listNewInput
            listNewInput.append(newEndNameType)
          }


      }

          while (start != 0){
            var startNameType: String = new String()
            var endNameType: String = new String()

//            if (v3._3.contains("\\d")) {
//              val array: Array[String] = v3._3.split("@")
//          }
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
  var reachLabel : scala.collection.mutable.Map[String, scala.collection.mutable.Set[String]] = scala.collection.mutable.Map[String, scala.collection.mutable.Set[String]]()
//  var yLabel : scala.collection.mutable.Set[VertexId] =  scala.collection.mutable.Set()
  var now : Boolean = false
}
