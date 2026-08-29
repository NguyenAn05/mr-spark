package task11

import java.io.{DataInput, DataOutput}
import org.apache.hadoop.io.Writable

final class AmountSummaryWritable() extends Writable {
  private var currentPurchaseCount = 0L
  private var currentAmountCount = 0L
  private var currentSumAmount = 0.0
  private var currentSumAmountSquared = 0.0

  def purchaseCount: Long = currentPurchaseCount
  def amountCount: Long = currentAmountCount
  def sumAmount: Double = currentSumAmount
  def sumAmountSquared: Double = currentSumAmountSquared

  def set(purchaseCount: Long, amountCount: Long, sumAmount: Double, sumAmountSquared: Double): Unit = {
    currentPurchaseCount = purchaseCount
    currentAmountCount = amountCount
    currentSumAmount = sumAmount
    currentSumAmountSquared = sumAmountSquared
  }

  override def write(output: DataOutput): Unit = {
    output.writeLong(currentPurchaseCount)
    output.writeLong(currentAmountCount)
    output.writeDouble(currentSumAmount)
    output.writeDouble(currentSumAmountSquared)
  }

  override def readFields(input: DataInput): Unit = {
    currentPurchaseCount = input.readLong()
    currentAmountCount = input.readLong()
    currentSumAmount = input.readDouble()
    currentSumAmountSquared = input.readDouble()
  }
}
