package scala.quantexaSolutions

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import java.util._

object SolutionExtraMarks {

  //EXTRA MARKS QUESTION::


    def flownTogether(spark: SparkSession,solution4:Dataset[Row],customeDs:Dataset[Row],atLeastNTimes: Int, from: Date, to: Date) :Dataset[Row] = {

     solution4.join (customeDs, solution4.col ("Passenger 1 ID") === customeDs.col ("passengerId") )
    .where (col ("date") between (from, to) ).filter (col ("Number of flights together") === atLeastNTimes).drop ("date")
    .drop (customeDs.col ("passengerId") )
    .withColumn ("from", lit (from) ).withColumn ("to", lit (to) )

   /* SolutionExtraMarks.show ()
    SolutionExtraMarks.coalesce (1).write.option ("header", "true") csv ("src\\main\\resources\\SolutionExtraMarksTest")*/
  }

}