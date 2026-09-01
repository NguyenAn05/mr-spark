package task12

import java.io.{DataInput, DataOutput}
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.io.WritableComparator

class VarietyKey extends WritableComparable[VarietyKey] {
  var month: String = ""
  var state: String = ""
  var style: String = ""
  var sku: String = ""

  def set(m: String, st: String, sty: String, sk: String): Unit = {
    month = m
    state = st
    style = sty
    sku = sk
  }

  override def write(out: DataOutput): Unit = {
    out.writeUTF(month)
    out.writeUTF(state)
    out.writeUTF(style)
    out.writeUTF(sku)
  }

  override def readFields(in: DataInput): Unit = {
    month = in.readUTF()
    state = in.readUTF()
    style = in.readUTF()
    sku = in.readUTF()
  }

  override def compareTo(o: VarietyKey): Int = {
    val cmpMonth = month.compareTo(o.month)
    if (cmpMonth != 0) return cmpMonth
    val cmpState = state.compareTo(o.state)
    if (cmpState != 0) return cmpState
    val cmpStyle = style.compareTo(o.style)
    if (cmpStyle != 0) return cmpStyle
    sku.compareTo(o.sku)
  }

  override def hashCode(): Int = (month, state, style, sku).hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case other: VarietyKey => month == other.month && state == other.state && style == other.style && sku == other.sku
    case _ => false
  }
}

object VarietyKey {
  def apply(m: String, st: String, sty: String, sk: String): VarietyKey = {
    val k = new VarietyKey()
    k.set(m, st, sty, sk)
    k
  }
}

class VarietyGroupComparator extends WritableComparator(classOf[VarietyKey], true) {
  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    val k1 = a.asInstanceOf[VarietyKey]
    val k2 = b.asInstanceOf[VarietyKey]
    val cmpMonth = k1.month.compareTo(k2.month)
    if (cmpMonth != 0) return cmpMonth
    val cmpState = k1.state.compareTo(k2.state)
    if (cmpState != 0) return cmpState
    k1.style.compareTo(k2.style)
  }
}

class VarietySortComparator extends WritableComparator(classOf[VarietyKey], true) {
  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    val k1 = a.asInstanceOf[VarietyKey]
    val k2 = b.asInstanceOf[VarietyKey]
    k1.compareTo(k2)
  }
}

class MedianKey extends WritableComparable[MedianKey] {
  var month: String = ""
  var state: String = ""

  def set(m: String, st: String): Unit = {
    month = m
    state = st
  }

  override def write(out: DataOutput): Unit = {
    out.writeUTF(month)
    out.writeUTF(state)
  }

  override def readFields(in: DataInput): Unit = {
    month = in.readUTF()
    state = in.readUTF()
  }

  override def compareTo(o: MedianKey): Int = {
    val cmpMonth = month.compareTo(o.month)
    if (cmpMonth != 0) return cmpMonth
    state.compareTo(o.state)
  }

  override def hashCode(): Int = (month, state).hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case other: MedianKey => month == other.month && state == other.state
    case _ => false
  }
}

object MedianKey {
  def apply(m: String, st: String): MedianKey = {
    val k = new MedianKey()
    k.set(m, st)
    k
  }
}
