package task11

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

object Task11Config {
  val DateColumn = 2
  val StatusColumn = 3
  val SizeColumn = 10
  val QuantityColumn = 13
  val AmountColumn = 15
  val StateColumn = 17

  val BoughtStatus = "shipped"
  val BoughtThreshold = 10000L
  val ShortWindow = 5
  val LongWindow = 10

  val InputDateFormatter: DateTimeFormatter = new DateTimeFormatterBuilder().appendPattern("MM-dd-").appendValueReduced(ChronoField.YEAR, 2, 2, 2000).toFormatter()
  val OutputDateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

  def parseDate(value: String): LocalDate = LocalDate.parse(value.trim, InputDateFormatter)
  def formatDate(value: LocalDate): String = value.format(OutputDateFormatter)
  def windowLength(totalBought: Long): Int = if (totalBought > BoughtThreshold) ShortWindow else LongWindow
}
