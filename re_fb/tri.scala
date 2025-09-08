
object Tri {
   def main(args: Array[String]) = {  // this is the entry point to our code
      val sc = getSC()  // one function to get the sc variable
      val myrdd = getFB(sc) // on function to get the rdd
      val counts = countTriangles(myrdd) // get the number of triangles
      //sadly we convert this single number into an rdd and save it to HDFS
      sc.parallelize(List(counts)).saveAsTextFile("NumberOfTriangles")  
  }

    def getSC() = { // get the spark context variable
    }

    def getFB(sc: SparkContext): RDD[(String, String)] = {
        // read the /datasets/facebook data and convert each line into an
        // rdd. Each entry of the RDD is a pair of strings, representing the ids
        // of nodes that are neighbors
        //
        // Remember to split by a single space, not by a tab
    }

    def makeRedundant(edgeList: RDD[(String, String)]): RDD[(String, String)] = {
        // An edge list is redundant if whenver an entry (a,b) appears, then so does (b,a)
        // If input is this:
        //    (1, 2)
        //    (3, 1) 
        //    (1, 2)
        //    (3, 2)
        // Then the output should be this (ordering of the rows does not matter)
        // (2, 1) 
        // (1, 2) 
        // (1, 3)
        // (3, 1)
        // (3, 2)
        // (2, 3)
        // note that there are no duplicates

        // use only one wide-dependency operation and that is a distinct()
        // at the end to get rid of duplicates. Do not think of this problem as searching
        // to see if the RDD has a (b,a)
        // This can be done using 2 total transformations
    }

   def noSelfEdges(edgeList: RDD[(String, String)]): RDD[(String, String)] = {
        //Gets rid of self-edges
        // If the input rdd is this:
        // (1, 2)
        // (1, 1)
        // The output RDD would be
        // (1, 2)
        
        // this can be done using 1 transformation
   }


   def friendsOfFriends(edgeList: RDD[(String, String)]): RDD[(String, (String, String))] = {
       // From the edge list, we want to know which nodes have friends in common
       // If an input RDD looks like this
       // (1, 2)
       // (2, 3)
       // (2, 1)
       // (3, 2)
       // We want the output to look like this
       // (2, (1, 3)    <---  this means there is an edge from 1 to 2 and an edge from 2 to 3
       // (2, (3, 1)    <--- this means there is an edge from 3 to 2 and from 2 to 1

       // this is the same as finding all paths of length 2 in the graph specified by edgeList.


       // You can essume that the input edgeList is in redundant form
       // you only need 1 wide dependency operation. In fact, you only need 1 transformation total.
   }

   def journeyHome(edgeList: RDD[(String, String)],  twoPaths:  RDD[(String, (String, String))]): RDD[((String, String), (String, Null))] = {
       // There are two input RDDs. The first is an edgeList, like this
       // (1, 2)
       // (1, 3)
       // (3, 1)
       // (4, 2)
       // (4, 1)
       // and the second is a list of paths of length 2 like this
       // (2, (1, 3))    <--- means there is a path from 1 to 2 to 3
       // (2, (3, 1))    <--- means there is a path from 3 to 2 to 1
       // (5, (1, 4))    <--- means there is a path from 1 to 5 to 4
       // (6, (4, 2))
       //
       // We would like to join together all entries from the first RDD that match the
       // last tuple of the second RDD. For example, we would like to 
       // match the (1,3) to (2, (1, 3)) and
       // match the (3,1) to (2, (3, 1))
       //
       // You will use join to do the match, but you will need to create some intermediate
       // RDDs and think carefully about what their keys and values should be.
       //
       //The output should look like this:
       //
       // ((1, 3), (2, null))  <---- this is the result of matching (1,3) to (2, (1, 3))
       // ((3, 1), (2, null))  <---- this is the result of matching (3,1) to (2, (3, 1))
       // ((4, 2), (6, null))  <---- this is the result of matching (2,4) to (6, (4, 2))
   } 

   def toyGraph(sc: SparkContext): RDD[(String, String)] = {
       // creates a toy graph for triangle counting
       //
       // 1 ----- 2
       // | \     |
       // |   \   |
       // |     \ |
       // 4-------3 ------ 5
       //
       // There are only 2 triangles (a triangle is a group of 3 nodes that have edges between them)
       //
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
                         // add some tricky things
                         ("1", "3"), // duplicate
                         ("3", "1"),
                         ("1", "1"),  //self edge
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
        // the edgelist could either come from getFB() or toyGraph()
        // In both cases, it is your responsibility to make sure the code
        // does not crash. If the input edgeList comes from toyGraph(),
        // your code should be returning 2 as the answer.
        val no_self_edges = noSelfEdges(edgeList) 
        val double_it = makeRedundant(no_self_edges) //ensure that we have a redundant edge list
        val fr = friendsOfFriends(double_it)
        val almostThere = journeyHome(double_it, fr)
        // entries of almostThere look something like this:
        //  ((1,3),(4, null))  <---- the graph represented by edgeList has a path from 1 to 4 to 3 to 1.
        //                           this is a triangle! yay!
        almostThere.count() // uh oh, this gives the wrong answer for some reason
                            // every entry in almostThere represents a triangle. No 
                            // triangles are missing, but when we count them, we get the wrong answer.
                            // your job is to find out why (examine what happens when the input is
                            // toyGraph()  ). There is an easy fix once you figure this out.

        // make sure to return, not print, the number of triangles
        //
        // if you want a challenge, there is a lot of redundancy contained in the intermediate RDDs
        // if you understand how this triangle counting works, you should be able to remove most
        // of this redundancies (i.e., make the intermediate RDDs smaller).
    }


}
                                                                                                                          


