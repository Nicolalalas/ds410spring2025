/* 
 * Q1: load this file into your spark-shell as follows
 
:load guide1.scala

 * (pay attention to the punctation). When you edit this file
 * you will need to reload it into spark-shell in the same way.
 */



/* 
 * Q2: This is how  you make a testing RDD:

val myList = List("id\tcolor\tprice",
                  "1\tred\t3.01",
                  "2\tblue\t9.99"
                 )
val myrdd = sc.parallelize(myList, 3) // this is the RDD

 * This is how you examine <= 5 rows of the rdd:

myrdd.take(5).foreach{x => println(x)}

 * Edit the lines below (outside of this comment block) so that
 * the test RDD has 6 or more lines. Add code to print out up to
 * 10 lines of the rdd
 */

val myList = List("id\tcolor\tprice",
                  "1\tred\t3.01",
                  "2\tblue\t9.99"
                 )
val myrdd = sc.parallelize(myList, 3) 


/* Q3
 * Next we want to use myrdd to create another RDD that looks like
 * ["id", "color", "price"]
 * ["1", "red", "3.01"]
 * ["2", "blue", "9.99"]
 * ...
 * That is, we want to split by tabs using map. In the code line (below
 * this comment block) that creates the rdd splitByTabs, modify the 
 * inside of the curly braces to do this. Use the split() method of strings
 *
 * Note: since splitByTabs is an rdd where each line is an array,
 * printing a few of its entries is more difficult. Instead of 
 * splitByTabs.take(5).foreach{x => println(x)}
 * you will need to do:
 
splitByTabs.take(5).map{x => "[" + x.mkString(", ") + "]"}.foreach{x=>println(x)}
 
 *
 */

val splitByTabs = myrdd.map{x => x} // change this

/* 
 * Q4
 * Now use filter to get rid of the header line, so that the resulting
 * RDD looks something like
 * ["1", "red", "3.01"]
 * ["2", "blue", "9.99"]
 * ...
 *
 * Remember that indexing in scala uses () instead of []
 */

val noheader = splitByTabs.filter{x => true} // change this


/*
 * Q5 
 * Now pull out the prices from the rdd and convert them to Doubles
 * to create an rdd called "prices"
 * (see scala slides for converting between types, do not use "cast")
 * The resulting RDD should look something like
 * 3.01
 * 9.99
 * ...
 * and spark-shell should be telling you that the type of
 * the "prices" rdd is RDD[Double] (not RDD[String])
 */

val prices = noheader.map{x => x} // change this


/*
 * Q6: map vs. flatMap
 * Write code to print out some of the entries of the rdd mystery1 below.
 * Write code to print out some of the entries of the rdd mystery2 below.
 * Note that the only difference between mystery1 and mystery2 is that
 * one was create by map and the other by flatMap. What is the difference
 * between map and flatMap?
 */
val mystery1 = noheader.map{ x => List(x(0), x(1))}
val mystery2 = noheader.flatMap{ x => List(x(0), x(1))}
