package scala.quantexaSolutions

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDate

case class Flightdata(passengerId:Int, flightId:Int, from:String, to:String, date:String)
case class Passengers(passengerId:Int, firstName:String,lastName:String)

class QuantexaMainSpec extends AnyFlatSpec {

  val spark = SparkSession.builder().master("local[*]").appName("sparkTest").getOrCreate()

  import spark.implicits._

  val FlightdataDS = spark.read.option("header", "true").option("inferSchema", "true").csv("src//flightData.csv").as[Flightdata]
  val PassengersDS = spark.read.option("header", "true").option("inferSchema", "true").csv("src//passengers.csv").as[Passengers]
  val solution4 = spark.read.option("header", "true").option("inferSchema", "true").csv("src\\main\\resources\\Solution4DS")
  val customeDs = FlightdataDS.drop("flightId").drop("from").drop("to")

  behavior of "Solution 1"

      it should "Find the total number of flights for each month." in {

        val actualDS =  Solution1.NumberOfFlights(spark, FlightdataDS)
        actualDS.show(4)
        val expectedDS = spark.read.option("header","true").csv("src\\main\\resources\\Solution1DS")
        assert(actualDS.count() === expectedDS.count())

      }

  it should "Find the names of the 100 most frequent flyers." in {

    val actualDS = Solution2.FrequentFlyer(spark, FlightdataDS, PassengersDS)
    actualDS.show(4)
    val expectedDS = spark.read.option("header", "true").csv("src\\main\\resources\\Solution2DS")
    assert(actualDS.count() === expectedDS.count())

  }

  it should "Find the greatest number of countries a passenger has been in without being in the UK." in {

    val actualDS = Solution3.LongestRun(spark, FlightdataDS)
    actualDS.show(4)
    val expectedDS = spark.read.option("header", "true").csv("src\\main\\resources\\Solution3DS")
    assert(actualDS.count() === expectedDS.count())

  }

  it should "Find the passengers who have been on more than 3 flights together." in {

    val actualDS = Solution4.NumberFlightTogether(spark, FlightdataDS, PassengersDS)
    //actualDS.show(4)
    val expectedDS = spark.read.option("header", "true").csv("src\\main\\resources\\Solution4DS")
    assert(actualDS.count() === expectedDS.count())

  }

  it should "Find the passengers who have been on more than N flights together within the range (from,to)." in {

    val actualDS = SolutionExtraMarks.flownTogether(spark,solution4,customeDs,7,java.sql.Date.valueOf(LocalDate.parse("2017-11-22")),java.sql.Date.valueOf(LocalDate.parse("2017-11-25")))
    actualDS.show(4)
    val expectedDS = spark.read.option("header", "true").csv("src\\main\\resources\\SolutionExtraMarksDS")
    assert(actualDS.count() === expectedDS.count())

  }
}