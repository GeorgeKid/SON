import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

import scala.collection.mutable.Map
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import java.io._


object Son{

  def checkKitem(preCan:Set[Set[String]], kitem: Set[String]): Boolean = {
    //var flag = true
    val subs = kitem.subsets(kitem.size-1)
    for(sub <- subs){
      if (preCan.contains(sub) == false){
        return false
      }
    }
    return true

  }


  def getKitem(baskets:List[Set[String]], preCan:Set[Set[String]],single:Set[Set[String]], support: Double): Set[Set[String]] = {
    var itemCount = Map[Set[String], Int]()
    var kitemfre = Set[Set[String]]()
    var kitemcan = Set[Set[String]]()

    var i = 0
    var j = 0

    for(pre <- preCan){
      for(singleitem <- single){
         val kitem = pre ++ singleitem
         if (kitem.size == pre.size+1 && checkKitem(preCan, kitem)){
            kitemcan += kitem
        }
      }
    }

    for (item <- kitemcan){
      for (basket <- baskets){
        if (item.subsetOf(basket)){
          if (itemCount.contains(item) == false){
            itemCount.put(item, 1)
          }else{
            itemCount(item) += 1
          }
        }
      }
    }

    for (i <- itemCount){
      if(i._2 >= support){
        kitemfre += i._1
      }
    }

    return kitemfre

  }

  def A_Priori (chunk:Iterator[List[String]], basketSize: Int, oldsupport: Int): Iterator[Set[String]]= {
    var candidates = Set[Set[String]]()
    var baskets = List[Set[String]]()
    val singleCount = Map[String, Int]()
    var single = Set[Set[String]]()
    var chunkSize = 0
    for (basket <- chunk){

      baskets = basket.toSet :: baskets
      chunkSize += 1
      for (item <- basket){
        if (singleCount.contains(item)==false){
          singleCount.put(item, 1)
        }else{
          singleCount(item) +=  1
        }
      }
    }
    val support = (oldsupport * chunkSize / basketSize ).asInstanceOf[Double]
    //record the fre_can, size = 1
    for(i <- singleCount){
      if (i._2 >= support){
         single += Set(i._1)
      }
    }
    candidates ++= single

    var precan = single
    var kcan = single

    while (kcan.isEmpty == false){
      kcan = getKitem(baskets, precan, single,  support)
      precan = kcan
      candidates ++= kcan

    }

    return candidates.iterator

  }


  def countSupport(chunk:Iterator[List[String]], candidates: Set[Set[String]]): Iterator[(Set[String], Int )] = {
    var baskets = ListBuffer[Set[String]]()
    val itemCount = Map[Set[String], Int]()
    while(chunk.hasNext){
      val basket = chunk.next()
      baskets += basket.toSet
    }

    for(basket <- baskets){
      for(item <- candidates){
        if (item.subsetOf(basket) == true){
          if (itemCount.contains(item)==false){
               itemCount.put(item, 1)
          }else{
               itemCount(item) +=  1
          }
        }
      }
    }

    return itemCount.iterator


  }

  def main (args: Array[String]): Unit ={
    val caseNum = args(0).toInt
    //val caseNum = 1
    val s = args(2).toInt
    //val s = 1200
    var conf = new SparkConf()
    conf.setAppName("Shiwei_Huang_Son")
    conf.setMaster("local[4]")

    val sc = new SparkContext (conf)

    var  chunkNum = 0

    if (s >= 10){
       chunkNum = 4
    }else{
       chunkNum = 1
    }
    //record start-time
    val start_time = System.currentTimeMillis()

    val input = sc.textFile(args(1), chunkNum)
    //val input = sc.textFile("./books.csv", 1) // 6 represents the num of partitions

    //val chunkNum = input.getNumPartitions //num of chunks, chunk is a subsets of whole baskets

    val header = input.first()

    var baskets: RDD[List[String]] = null
    if (caseNum == 1){
      baskets = input.filter(x => x!= header)
        .map(x => x.split(",")).map(x => (x(0), Set(x(1))))
        .reduceByKey(_++_)
        .map(x => x._2.toList).cache()
    }else{
      baskets = input.filter(x => x!= header)
        .map(x => x.split(",")).map(x => (x(1), Set(x(0))))
        .reduceByKey(_++_)
        .map(x => x._2.toList).cache()

    }
    //begin of SON algorithm
    val basketsize = baskets.collect().size
    //var map = baskets.mapPartitions( x => println(x.toList.size))
    //s*(x.toList.size / basketsize )
    var map1 = baskets.mapPartitions(x => A_Priori(x, basketsize, s))

    var reduce1 = map1.cache().collect().toSet

    var map2 = baskets.mapPartitions(x => countSupport(x, reduce1)).map(x => (x._1, x._2))

    var reduce2 = map2.reduceByKey((x, y) => x+y)

    val result = reduce2.filter(x => x._2 >= s).map(x => x._1.toList.sorted)

    val output = result.map(x => (x.size, x)).sortBy(x => String.join("", x._2)).map(x =>(x._1, "(\'"+x._2.mkString("\', \'")+"\')")).groupByKey().sortByKey().map(x => x._2.mkString(", ")).collect().toList

    var filename = ""

    if (args(1).split("\\.")(0)=="small2"){
      filename = "Shiwei_Huang_SON_"+args(1).charAt(0).toUpper+args(1).split("\\.")(0).substring(1)+".case"+args(0).toString+".txt"

    }else{
      filename = "Shiwei_Huang_SON_"+args(1).charAt(0).toUpper+args(1).split("\\.")(0).substring(1)+".case"+args(0).toString+"-"+s.toString+".txt"
    }

    val file = new File(filename)

    val out = new PrintWriter(file)


    for(line <- output.toList){
      out.write(line+"\n"+"\n")
    }

    out.close()

    val end_time = System.currentTimeMillis()
    println("===================Count Complete, Total Time: " + (end_time - start_time) / 1000 + " secs====================")

  }

}