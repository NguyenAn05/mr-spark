package task11

object CsvParser {
  def parseLine(line: String): Vector[String] = {
    val fields = Vector.newBuilder[String]
    val current = new StringBuilder
    var inQuotes = false
    var index = 0

    while (index < line.length) {
      val ch = line.charAt(index)
      if (ch == '"') {
        if (inQuotes && index + 1 < line.length && line.charAt(index + 1) == '"') {
          current.append('"')
          index += 1
        } else {
          inQuotes = !inQuotes
        }
      } else if (ch == ',' && !inQuotes) {
        fields += current.result()
        current.clear()
      } else {
        current.append(ch)
      }
      index += 1
    }

    fields += current.result()
    fields.result()
  }

  def escapeField(value: String): String = {
    val safeValue = Option(value).getOrElse("")
    if (safeValue.exists(ch => ch == ',' || ch == '"' || ch == '\n' || ch == '\r')) {
      '"' + safeValue.replace("\"", "\"\"") + '"'
    } else safeValue
  }

  def formatRow(values: Seq[String]): String = values.map(escapeField).mkString(",")
}
