package scala.quantexaSolutions

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Solution1 {


  def NumberOfFlights(spark:SparkSession, FlightdataDS: Dataset[Flightdata]):DataFrame = {


    val newFlightdataDS = FlightdataDS.withColumn("Month", date_format(to_date(col("date"), "yyyy-MM-dd"), "MM"))

    newFlightdataDS.groupBy(col("Month")).agg(count("*").alias("Number of Flights"))

    /*solution1.show(4)
    solution1.coalesce(1).write.option("header","true").csv("src\\main\\resources\\Solution1DSTest")*/

  }

}
