package scala.quantexaSolutions

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object Solution2 {

  def FrequentFlyer(spark: SparkSession,FlightdataDS:Dataset[Flightdata],PassengersDS:Dataset[Passengers]):Dataset[Row] = {

    val solDS = FlightdataDS.groupBy("passengerId")
      .agg(count("*").alias("Number of Flights"))

     solDS.join(PassengersDS, solDS.col("passengerId") === PassengersDS.col("passengerId"))
      .drop(PassengersDS.col("passengerId")).limit(100).sort(col("Number of Flights").desc)


      //solution2.coalesce(1).write.option("header","true")csv("src\\main\\resources\\Solution2DSTest")

  }

}
