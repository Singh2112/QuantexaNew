package scala.quantexaSolutions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import quantexaSolutions.Solution1.NumberOfFlights
import quantexaSolutions.Solution2.FrequentFlyer
import quantexaSolutions.Solution3.LongestRun
import java.time.LocalDate
import quantexaSolutions.Solution4.NumberFlightTogether
import quantexaSolutions.SolutionExtraMarks.flownTogether


//CASE CLASS FOR DATASET SCHEMA IN ORDER TO MAKE OT TYPED SAFE
case class Flightdata(passengerId:Int, flightId:Int, from:String, to:String, date:String)
case class Passengers(passengerId:Int, firstName:String,lastName:String)

object QuantexaSolu {

  def main(args: Array[String]): Unit = {

    try {
      Logger.getLogger("org").setLevel(Level.ERROR)
      System.setProperty("hadoop.home.dir", "C:\\winutils")
      val spark = SparkSession.builder().master("local[*]").appName("spark").getOrCreate()

      //TO MAKE  CSV FORMAT FOR WRITE
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      fs.setWriteChecksum(false)
      import spark.implicits._

      //DATASETS:
      val FlightdataDS = spark.read.option("header", "true").option("inferSchema", "true").csv("src//flightData.csv").as[Flightdata]
      val PassengersDS = spark.read.option("header", "true").option("inferSchema", "true").csv("src//passengers.csv").as[Passengers]
      val solution4 =   spark.read.option("header","true").option("inferSchema","true").csv("src\\main\\resources\\Solution4DS")
      val customeDs = FlightdataDS.drop("flightId").drop("from").drop("to")


      //SOLUTION 1: Find the total number of flights for each month.
      NumberOfFlights(spark, FlightdataDS) //.show(5)

      //SOLUTION 2:Find the names of the 100 most frequent flyers.
      FrequentFlyer(spark, FlightdataDS, PassengersDS)

      //SOLUTION 3:Find the greatest number of countries a passenger has been in without being in the UK.
      LongestRun(spark, FlightdataDS)

      //SOLUTION 4 : Find the passengers who have been on more than 3 flights together.
      NumberFlightTogether(spark, FlightdataDS, PassengersDS)

      //SOLUTION FOR EXTRA MARKS: Find the passengers who have been on more than N flights together within the range (from,to).
      flownTogether(spark,solution4,customeDs,7,java.sql.Date.valueOf(LocalDate.parse("2017-11-22")),java.sql.Date.valueOf(LocalDate.parse("2017-11-25")))

    }
      catch {
      case e :Exception =>
        println("The Exception has occurred " + e.printStackTrace+ " due to " +e.getMessage)

    }

  }

}
