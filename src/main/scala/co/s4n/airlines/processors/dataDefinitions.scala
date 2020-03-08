package co.s4n.airlines.processors

case class AirlineDelay(FL_DATE: String,
                        OP_CARRIER: String,
                        ORIGIN: String,
                        DEST: String,
                        DEP_DELAY: Option[String],
                        ARR_DELAY: Option[String],
                        DEP_TIME:String)

case class AirlineStats(name: String,
                        totalFlights: Long,
                        largeDelayFlights: Long,
                        smallDelayFlights: Long,
                        onTimeFlights: Long)

case class FlightsStats(destination: String,
                        morningFlights: Long,
                        afternoonFlights: Long,
                        nightFlights: Long)

case class CancelledFlight(number: Int,
                           origin: String,
                           destination: String,
                           total:Long,
                           causes: List[(String, Long)])