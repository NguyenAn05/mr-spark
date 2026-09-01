package task12

import scala.util.Try

case class StyleRecord(month: String, state: String, style: String, sku: String, sizeRank: Int)

object Task12RecordParser {
  def parseRecord(line: String): Option[StyleRecord] = {
    if (line == null || line.trim.isEmpty || line.startsWith("index,")) None
    else {
      val fields = task11.CsvParser.parseLine(line)
      val required = Task12Config.StateColumn

      if (fields.length <= required) None
      else {
        val dateStr = fields(Task12Config.DateColumn).trim
        val style = fields(Task12Config.StyleColumn).trim
        val sku = fields(Task12Config.SkuColumn).trim
        val stateStr = fields(Task12Config.StateColumn).trim
        val sizeStr = fields(Task12Config.SizeColumn).trim

        for {
          date <- Try(Task12Config.parseDate(dateStr)).toOption
          if style.nonEmpty
          if sku.nonEmpty
          if stateStr.nonEmpty
          if sizeStr.nonEmpty
        } yield {
          val month = Task12Config.formatMonth(date)
          val state = Task12Config.normalizeState(stateStr)
          val sizeRank = Task12Config.parseSizeRank(sizeStr)
          StyleRecord(month, state, style, sku, sizeRank)
        }
      }
    }
  }
}
