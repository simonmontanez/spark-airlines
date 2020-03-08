package co.s4n.airlines

import co.s4n.airlines.processors.{AirlineDelay, AirlinesDataStats, FlightsDataStats}
import org.apache.spark.sql.{DataFrame, Dataset}

class DataAirlinesAnalyser extends AirlinesDataStats with FlightsDataStats with SparkSessionWrapper{

  import spark.implicits._

  val dataFrame: DataFrame = loadDataSet()

  val dataSet: Dataset[AirlineDelay] = dataFrame.as[AirlineDelay]

  def loadDataSet(): DataFrame =  {
    spark
      .read
      .option("header", true)
      .csv("data-sets/airline-delay-and-cancellation/2009.csv",
        "data-sets/airline-delay-and-cancellation/2010.csv",
        "data-sets/airline-delay-and-cancellation/2011.csv",
        "data-sets/airline-delay-and-cancellation/2012.csv",
        "data-sets/airline-delay-and-cancellation/2013.csv",
        "data-sets/airline-delay-and-cancellation/2014.csv")
  }

}
