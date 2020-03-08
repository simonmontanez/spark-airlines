package co.s4n.airlines.processors

import co.s4n.airlines.DataAirlinesAnalyser
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

trait AirlinesDataStats {

  this: DataAirlinesAnalyser =>

  import spark.implicits._

  /*
   * ARR_DELAY < 5 min --- On time
   * 5 > ARR_DELAY < 45min -- small Delay
   * ARR_DELAY > 45min large delay
   *
   */
  def delayedAirlines(yearOption: Option[String]): Seq[AirlineStats] = {

    val parseYear = (date: String) => DateTime.parse(date).getYear.toString

    val dataSetAirlines = yearOption.fold(dataSet)(year => dataSet.filter(d => parseYear(d.FL_DATE) == year))

    dataSetAirlines.groupBy("OP_CARRIER")
      .agg(count("*") as "totalFlights",
        count(when('ARR_DELAY <= 5, true)) as "onTimeFlights",
        count(when('ARR_DELAY > 5 && 'ARR_DELAY <= 45, true)) as "smallDelayFlights",
        count(when('ARR_DELAY > 45,true)) as "largeDelayFlights")
      .withColumnRenamed("OP_CARRIER", "name")
      .sort('largeDelayFlights, 'smallDelayFlights, 'onTimeFlights)
      .as[AirlineStats].collect()
  }

}
