package scala.quantexaSolutions

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object Solution4 {

  def NumberFlightTogether(spark: SparkSession,FlightdataDS:Dataset[Flightdata],PassengersDS:Dataset[Passengers]) :Dataset[Row] ={

    val Fightnamechange = FlightdataDS.withColumnRenamed("passengerId", "Passenger 1 ID")
    val Passnamechange = PassengersDS.withColumnRenamed("passengerId", "Passenger 2 ID")
    val customeDs = FlightdataDS.drop("flightId").drop("from").drop("to")

    val solu4Join = Fightnamechange.join(Passnamechange, Fightnamechange.col("Passenger 1 ID") =!= Passnamechange.col("Passenger 2 ID"))
                    .select(Fightnamechange.col("Passenger 1 ID"), Fightnamechange.col("flightId"),
                      Passnamechange.col("Passenger 2 ID"))


    solu4Join.groupBy("Passenger 1 ID", "Passenger 2 ID")
              .agg(count("*").alias("Number of flights together"))
              .where(col("Number of flights together") > 3)

       //solution4.coalesce(1).write.option("header", "true").csv("src\\main\\resources\\Solution4DSTest")

  }
}
