package co.s4n.airlines.processors

import co.s4n.airlines.DataAirlinesAnalyser
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

trait FlightsDataStats {

  this: DataAirlinesAnalyser =>

  import spark.implicits._

  /**
   * 00:00 y 8:00 - Morning
   * 8:01 y 16:00 - Afternoon
   * 16:01 y 23:59 - Night
   *
   */
  def destinations(origin: String): Seq[FlightsStats] = {

    dataSet
      .filter(d => d.ORIGIN == origin)
      .groupBy("DEST")
      .agg(
        count( when('DEP_TIME >= 0 && 'DEP_TIME <= 800, true) ) as "morningFlights",
        count( when('DEP_TIME >= 801 && 'DEP_TIME <= 1600, true)) as "afternoonFlights",
        count( when('DEP_TIME >= 1601 && 'DEP_TIME <= 2359, true)) as "nightFlights"
      )
      .withColumnRenamed("DEST", "destination")
      .as[FlightsStats].collect()
  }

  def flightInfo(topLimit:Int): Array[CancelledFlight] = {

    val mapCauseCode = udf((value: String) => {
      value match {
        case "A" => "Airline/Carrier"
        case "B" => "Weather"
        case "C" => "National Air System"
        case "D" => "Security"
        case _ => value
      }
    })

    dataFrame // Using DF just to test filters and expressions
      .select('OP_CARRIER_FL_NUM cast ("int") as "number",
              'ORIGIN as "origin",
              'DEST as "destination",
              'CANCELLED as "cancelled",
              mapCauseCode('CANCELLATION_CODE) as "causeCode")

      .filter('CANCELLED === 1)
      .groupBy('number, 'origin, 'destination, 'causeCode)
      .agg(count("causeCode") as "totalCauses")

      .groupBy('number, 'origin, 'destination)
      .agg(sum('totalCauses) as "total", collect_list(struct('causeCode, 'totalCauses)) as "causes")
      .sort('total.desc)

      .limit(topLimit)
      .as[CancelledFlight]
      .collect()
  }


  def daysWithDelays(): Array[(String, Long)] = {
    val parseDay = (date: String) => DateTime.parse(date).dayOfWeek().getAsText

    dataSet
      .filter(_.ARR_DELAY.exists(_.toDouble > 45))
      .map( x => parseDay(x.FL_DATE))
      .withColumn("day", 'value)
      .groupBy('day)
      .agg(count("*") as "total")
      .as[(String, Long)]
      .collect()

  }

}
