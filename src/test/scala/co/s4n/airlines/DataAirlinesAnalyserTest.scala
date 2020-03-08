package co.s4n.airlines


import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec

class DataAirlinesAnalyserTest
  extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  val dataAnalyser = new DataAirlinesAnalyser()

  describe("DataAirlinesAnalyser airlines ") { // 1. ¿Cuáles son las aerolíneas más cumplidas y las menos cumplidas de un año en especifico?

    it("returns all airlines stats") {

      val delayedAirlines = dataAnalyser.delayedAirlines(Option.empty)

      spark.createDataFrame(delayedAirlines).show()

      assert(delayedAirlines.nonEmpty)

    }

    it("returns airlines stats by year") {

      val delayedAirlines = dataAnalyser.delayedAirlines(Some("2009"))

      spark.createDataFrame(delayedAirlines).show()

      assert(delayedAirlines.nonEmpty)

    }

  }

  describe("DataAirlinesAnalyser flights") {

    it("returns destinations stats by origin") { // 2. Dado un origen por ejemplo DCA (Washington), ¿Cuáles son destinos y cuantos vuelos presentan durante la mañana, tarde y noche?

      val flights = dataAnalyser.destinations("ATL")

      spark.createDataFrame(flights).show()

      assert(flights.nonEmpty)

    }

    it("returns flight cancellations") { // 3. Encuentre ¿Cuáles son los números de vuelo (top 20)  que han tenido más cancelaciones y sus causas?

      val flights = dataAnalyser.flightInfo(20)

      spark.createDataFrame(flights).show(false)

      assert(flights.nonEmpty)
    }

    it("returns days of week with total delays") { // 4. ¿Que dias se presentan más retrasos históricamente?

      val daysData = dataAnalyser.daysWithDelays()

      spark.createDataFrame(daysData).show(false)

      assert(daysData.nonEmpty)
    }

  }

}
