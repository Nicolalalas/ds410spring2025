import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Tri {
   def main(args: Array[String]) = {
      val sc = getSC()
      val myrdd = getFB(sc)
      val counts = countTriangles(myrdd)
      sc.parallelize(List(counts)).saveAsTextFile("NumberOfTriangles")  
  }

    def getSC() = {
        val conf = new SparkConf().setAppName("Triangle Counter")
        val sc = new SparkContext(conf)
        sc
    }

    def getFB(sc: SparkContext): RDD[(String, String)] = {
        sc.textFile("/datasets/facebook").map(line => {
            val parts = line.split(" ")
            (parts(0), parts(1))
        })
    }

    def makeRedundant(edgeList: RDD[(String, String)]): RDD[(String, String)] = {
        edgeList.flatMap { case (a, b) => List((a, b), (b, a)) }.distinct()
    }

   def noSelfEdges(edgeList: RDD[(String, String)]): RDD[(String, String)] = {
        edgeList.filter { case (a, b) => a != b }
   }

   def friendsOfFriends(edgeList: RDD[(String, String)]): RDD[(String, (String, String))] = {
       edgeList.join(edgeList).map { case (middle, (start, end)) => (middle, (start, end)) }
   }

   def journeyHome(edgeList: RDD[(String, String)],  twoPaths:  RDD[(String, (String, String))]): RDD[((String, String), (String, Null))] = {
       val edgeListKeyed = edgeList.map { case (a, b) => ((a, b), null) }
       val twoPathsKeyed = twoPaths.map { case (middle, (start, end)) => ((start, end), middle) }
       twoPathsKeyed.join(edgeListKeyed).map { case (edge, (middle, nullVal)) => (edge, (middle, nullVal)) }
   } 

   def toyGraph(sc: SparkContext): RDD[(String, String)] = {
       val mylist = List[(String, String)](
                         ("1", "2"),
                         ("2", "1"),
                         ("2", "3"),
                         ("3", "2"),
                         ("1", "3"),
                         ("3", "1"),
                         ("1", "4"),
                         ("4", "1"),
                         ("4", "3"),
                         ("3", "4"),
                         ("3", "5"),
                         ("5", "3"),
                         ("1", "3"),
                         ("3", "1"),
                         ("1", "1"),
                         ("3", "5"),
                         ("5", "3"),
                         ("1", "3"),
                         ("3", "1"),
                         ("1", "4"),
                         ("4", "1"),
                         ("4", "3")
                        )
        sc.parallelize(mylist, 2)
    }

    def countTriangles(edgeList: RDD[(String, String)]) = {
        val no_self_edges = noSelfEdges(edgeList) 
        val double_it = makeRedundant(no_self_edges)
        val fr = friendsOfFriends(double_it)
        val almostThere = journeyHome(double_it, fr)
        almostThere.count() / 6
    }
}
