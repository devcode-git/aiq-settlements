package com.devcode.accountiq.settlement.elastic.reports.settlement

object SettlementDetailReportField extends Enumeration {
  type SettlementDetailReportField = Value
  val companyAccount = Value("Company Account")
  val merchantAccount = Value("Merchant Account")
  val pspReference = Value("Psp Reference")
  val merchantReference = Value("Merchant Reference")
  val paymentMethod = Value("Payment Method")
  val creationDate = Value("Creation Date") // example 03/08/2023 14:37
  val timeZone = Value("TimeZone")
  val `type` = Value("Type")
  val modificationReference = Value("Modification Reference")
  val grossCurrency = Value("Gross Currency")
  val grossDebit = Value("Gross Debit (GC)")
  val grossCredit = Value("Gross Credit (GC)")
  val exchangeRate = Value("Exchange Rate")
  val netCurrency = Value("Net Currency")
  val netDebit = Value("Net Debit (NC)")
  val netCredit = Value("Net Credit (NC)")
  val commission = Value("Commission (NC)")
  val markup = Value("Markup (NC)")
  val schemeFees = Value("Scheme Fees (NC)")
  val interchange = Value("Interchange (NC)")
  val paymentMethodVariant = Value("Payment Method Variant")
  val modificationMerchantReference = Value("Modification Merchant Reference")
  val batchNumber = Value("Batch Number")
  val Reserved4 = Value("Reserved4")
  val Reserved5 = Value("Reserved5")
  val Reserved6 = Value("Reserved6")
  val Reserved7 = Value("Reserved7")
  val Reserved8 = Value("Reserved8")
  val Reserved9 = Value("Reserved9")
  val Reserved10 = Value("Reserved10")
  val shopperReference = Value("Shopper Reference")

  implicit def fieldToString(field: SettlementDetailReportField): String = field.toString
}
