package task11

import java.time.LocalDate

final case class BoughtOrder(
    state: String,
    purchaseDate: LocalDate,
    size: String,
    quantity: Long,
    amount: Option[Double]
)

final case class AmountSummary(purchaseCount: Long, amountCount: Long, sumAmount: Double, sumAmountSquared: Double) {
  def +(other: AmountSummary): AmountSummary = AmountSummary(purchaseCount + other.purchaseCount, amountCount + other.amountCount, sumAmount + other.sumAmount, sumAmountSquared + other.sumAmountSquared)

  def populationVariance: Option[Double] =
    if (amountCount == 0L) None
    else {
      val mean = sumAmount / amountCount.toDouble
      Some(math.max(0.0, sumAmountSquared / amountCount.toDouble - mean * mean))
    }
}

final case class SizeCandidate(state: String, windowDate: LocalDate, size: String, summary: AmountSummary, windowLength: Int)
