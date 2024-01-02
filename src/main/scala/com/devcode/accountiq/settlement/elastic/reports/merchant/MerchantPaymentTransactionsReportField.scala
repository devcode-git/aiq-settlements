package com.devcode.accountiq.settlement.elastic.reports.merchant

object MerchantPaymentTransactionsReportField extends Enumeration {
  type MerchantPaymentTransactionsReportField = Value
  val operator = Value("operator")
  val provider = Value("provider")
  val txRef = Value("txRef")
  val txId = Value("txId")
  val providerRef = Value("providerRef")
  val created = Value("created")
  val booked = Value("booked")
  val amount = Value("amount")
  val amountBase = Value("amountBase")
  val txAmount = Value("txAmount")
  val txAmountBase = Value("txAmountBase")
  val fee = Value("fee")
  val feeBase = Value("feeBase")
  val method = Value("method")
  val txType = Value("txType")
  val userId = Value("userId")
  val jurisdiction = Value("jurisdiction")

  implicit def fieldToString(field: MerchantPaymentTransactionsReportField): String = field.toString
}
