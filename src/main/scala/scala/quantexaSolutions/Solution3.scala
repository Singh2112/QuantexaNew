package scala.quantexaSolutions

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Solution3 {

  def LongestRun(spark:SparkSession,FlightdataDS:Dataset[Flightdata]) : DataFrame={

     FlightdataDS.select("*").where(col("from") =!= "ir" && col("to") =!= "ir")
      .drop("flightId")
      .drop("date").drop("to").groupBy("passengerId")
      .agg(count("*").alias("Longest Run"))


    /*solution3.show(4)
    solution3.coalesce(1).write.option("header","true").csv("src\\main\\resources\\Solution3DSTest")*/

  }

}
