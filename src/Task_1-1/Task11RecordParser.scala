package task11

import scala.util.Try

object Task11RecordParser {
  def parseBought(line: String): Option[BoughtOrder] = {
    if (line == null || line.trim.isEmpty || line.startsWith("index,")) None
    else {
      val fields = CsvParser.parseLine(line)
      val required = Task11Config.StateColumn

      if (fields.length <= required) None
      else {
        val status = fields(Task11Config.StatusColumn).trim.toLowerCase
        val state = fields(Task11Config.StateColumn).trim
        val size = fields(Task11Config.SizeColumn).trim
        val amount =
          Try(fields(Task11Config.AmountColumn).trim.toDouble).toOption
            .filter(value => java.lang.Double.isFinite(value))

        for {
          quantity <- Try(fields(Task11Config.QuantityColumn).trim.toLong).toOption
          date <- Try(Task11Config.parseDate(fields(Task11Config.DateColumn))).toOption
          if status.contains(Task11Config.BoughtStatus)
          if quantity != 0L
          if state.nonEmpty
          if size.nonEmpty
        } yield BoughtOrder(state, date, size, quantity, amount)
      }
    }
  }
}
