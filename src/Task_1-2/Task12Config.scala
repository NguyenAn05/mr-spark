package task12

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

object Task12Config {
  val DateColumn = 2
  val StyleColumn = 7
  val SkuColumn = 8
  val SizeColumn = 10
  val StateColumn = 17

  val InputDateFormatter: DateTimeFormatter = new DateTimeFormatterBuilder().appendPattern("MM-dd-").appendValueReduced(ChronoField.YEAR, 2, 2, 2000).toFormatter()
  
  def parseDate(value: String): LocalDate = LocalDate.parse(value.trim, InputDateFormatter)
  
  def formatMonth(date: LocalDate): String = f"${date.getYear}%04d-${date.getMonthValue}%02d"

  def normalizeState(state: String): String = {
    state.trim.toUpperCase
  }

  // XS=1, S=2, M=3, L=4, XL=5, XXL=6, 3XL=7, 4XL=8, 5XL=9, 6XL=10, Free=0
  def parseSizeRank(size: String): Int = {
    val upper = size.trim.toUpperCase
    upper match {
      case "FREE" => 0
      case "XS" => 1
      case "S" => 2
      case "M" => 3
      case "L" => 4
      case "XL" => 5
      case "XXL" => 6
      case "3XL" => 7
      case "4XL" => 8
      case "5XL" => 9
      case "6XL" => 10
      case _ => -1 // Unknown size
    }
  }

  val TargetSizeRank = 6 // XXL rank
}
