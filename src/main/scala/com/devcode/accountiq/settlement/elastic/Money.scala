package com.devcode.accountiq.settlement.elastic

import java.math.BigDecimal
import java.util.Currency
import scala.math.BigDecimal.RoundingMode
import scala.jdk.CollectionConverters._
import scala.util.Try

case class Money(value: Double, cy: String) {

  def negate: Money = Money(this.value * -1, cy)

  def abs: Money = Money(this.value.abs, cy)

  def +(m: Money): Money = {
    require(null != m && this.cy == m.cy)
    Money(this.value + m.value, m.cy)
  }

  def -(m: Money): Money = {
    require(null != m && this.cy == m.cy)
    Money(this.value - m.value, m.cy)
  }

  def gt(value: Long): Boolean = this.value > value

  def lt(value: Long): Boolean = !gt(value)

  override def toString: String = s"$value $cy"

  def formatedString() = s"${this.valueString} $cy"

  def valueString: String = "%.2f".format(value / 100.0)

  def isZero: Boolean = this.value == 0L

  def isNegative: Boolean = this.value < 0L

  def isPositive: Boolean = this.value > 0L

  override def equals(obj: scala.Any): Boolean =
    obj match {
      case money: Money => money.value == this.value && money.cy == this.cy
      case _            => false
    }

}

object Money {

  private[Money] val currencySymbolLookup: Map[String, String] = Map(
    "$" -> "USD",
    "€" -> "EUR",
    "£" -> "GBP",
    "¥" -> "JPY"
  )

  // supports format '200.00 EUR'
  def parse(strAmount: String): Money = {
    val ams                = strAmount.split(" ")
    val noDecimalPointCurr = getCurrency(ams.last).getDefaultFractionDigits == 0
    val amount             = ams.init.mkString match {
      case noCommas if !noCommas.contains(',') => noCommas

      // comma as thousands delimiter
      case multipleCommas if multipleCommas.contains(',') && (multipleCommas.contains('.') || noDecimalPointCurr) =>
        multipleCommas.replace(",", "")

      // comma as decimal delimiter
      case oneComma if oneComma.count(_ == ',') == 1 && !oneComma.contains('.')                                   =>
        oneComma.replace(',', '.')

    }

    parse(amount, ams.last)
  }

  def parse(realAmount: String, currencyCode: String): Money = {
    val cy     = getCurrency(currencyCode)
    val amount = realAmount.toDouble
    Money(amount, cy.getCurrencyCode)
  }

  @throws(classOf[java.lang.IllegalArgumentException])
  @scala.annotation.tailrec
  private def getCurrency(ofStr: String): Currency =
    ofStr match {
      case supportedSymbol if currencySymbolLookup contains supportedSymbol =>
        getCurrency(currencySymbolLookup(supportedSymbol))
      case currencyCode                                                     =>
        Currency.getInstance(currencyCode)
    }

}
