//This file does not need to be edited.

/*
 * Q1:
 * Open up the spark-shell in the sparkguide directory.
 * Load wordcount.scala (use :load wordcount.scala and pay attention
 * to punctuation)
 */


/* 
 * Q2:
 * In a separate ssh session,
 * edit the getTestRDD fundtion inside wordcount.scala to add more
 * text lines to the test rdd. Then go back to the spark shell and
 * reload wordcount.scala
 */

/*
 * Q3: Do this from spark-shell.
 * Using the sc variable that spark-shell already provides you, 
 * call the WordCount.getTestRDD function you just edited.
 * Put the return value in a val called myTestRdd
 */

/*
 * Q4:
 * In the spark-shell, print out some lines of myTestRDD
 */


/*
 * Q5: in the spark-shell, call the WordCount.doWordCount function
 * on myTestRDD. Put the return value in a val called myResult.
 */


/* Q6: in the spark-shell, print out some lines of myResult.
 * This is a simple way of interactively deveoping your code
 */


/* Q7: exit spark-shell. Using the spark (not scala) slides, compile 
 * the code. Using the spark (not scala) slides, run the code.
 * It will save the result in hdfs in a folder called "result"
 * Using HDFS commands, check out a few lines of this output.
 * 
 * Note 1: you will need to add a build.sbt file. 
 * Note 2: you will need to exclude guide1.scala from compilation 
 *    since it is not supposed to be compiled. To do this,
 *    add the following line at the end of build.sbt:

unmanagedSources / excludeFilter  ~= { _ || "guide1.scala" }

 */
