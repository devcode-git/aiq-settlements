package com.devcode.accountiq.settlement.elastic

import com.devcode.accountiq.settlement.transformer.AIQParserUtil


import java.util.Date

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

case class SettlementDetailReport(companyAccount: String,
                                  merchantAccount: String,
                                  pspReference: String,
                                  merchantReference: Long,
                                  paymentMethod: String,
                                  creationDate: Date,
                                  timeZone: String,
                                  `type`: String,
                                  modificationReference: String,
                                  grossCurrency: String,
                                  grossDebit: Option[Double],
                                  grossCredit: Double,
                                  exchangeRate: Double,
                                  netCurrency: String,
                                  netDebit: Option[Double],
                                  netCredit: Double,
                                  commission: Double,
                                  markup: Option[String],
                                  schemeFees: Option[String],
                                  interchange: Option[String],
                                  paymentMethodVariant: String,
                                  modificationMerchantReference: String,
                                  batchNumber: Long,
                                  shopperReference: Long
                                 )

object SettlementDetailReport {
  def fromESDocRaw(esDoc: ESDoc): SettlementDetailReport = {
    val doc = esDoc.doc
    SettlementDetailReport(
      doc(SettlementDetailReportField.companyAccount),
      doc(SettlementDetailReportField.merchantAccount),
      doc(SettlementDetailReportField.pspReference),
      doc(SettlementDetailReportField.merchantReference).toLong,
      doc(SettlementDetailReportField.paymentMethod),
      AIQParserUtil.dateFormat.parse(doc(SettlementDetailReportField.creationDate)),
      doc(SettlementDetailReportField.timeZone),
      doc(SettlementDetailReportField.`type`),
      doc(SettlementDetailReportField.modificationReference),
      doc(SettlementDetailReportField.grossCurrency),
      Option(doc(SettlementDetailReportField.grossDebit)).flatMap(s => if (s.isEmpty) None else Some(s)).map(_.toDouble),
      doc(SettlementDetailReportField.grossCredit).toDouble,
      doc(SettlementDetailReportField.exchangeRate).toDouble,
      doc(SettlementDetailReportField.netCurrency),
      Option(doc(SettlementDetailReportField.netDebit)).flatMap(s => if (s.isEmpty) None else Some(s)).map(_.toDouble),
      doc(SettlementDetailReportField.netCredit).toDouble,
      doc(SettlementDetailReportField.commission).toDouble,
      Option(doc(SettlementDetailReportField.markup)),
      Option(doc(SettlementDetailReportField.schemeFees)),
      Option(doc(SettlementDetailReportField.interchange)),
      doc(SettlementDetailReportField.paymentMethodVariant),
      doc(SettlementDetailReportField.modificationMerchantReference),
      doc(SettlementDetailReportField.batchNumber).toLong,
      doc(SettlementDetailReportField.shopperReference).toLong
    )
  }

  //  val settlementDetailReportMapping: Map[String, EntercashField.Value] = Map(
  //    "Company Account" -> EntercashField.String,
  //    "Merchant Account" -> EntercashField.String,
  //    "Psp Reference" -> EntercashField.String,
  //    "Merchant Reference" -> EntercashField.Long,
  //    "Payment Method" -> EntercashField.String,
  //    "Creation Date" -> EntercashField.Date,
  //    "TimeZone" -> EntercashField.String,
  //    "Type" -> EntercashField.String,
  //    "Modification Reference" -> EntercashField.String,
  //    "Gross Currency" -> EntercashField.String,
  //    "Gross Debit (GC)" -> EntercashField.Double,
  //    "Gross Credit (GC)" -> EntercashField.Double,
  //    "Exchange Rate" -> EntercashField.Double,
  //    "Net Currency" -> EntercashField.String,
  //    "Net Debit (NC)" -> EntercashField.Double,
  //    "Net Credit (NC)" -> EntercashField.Double,
  //    "Commission (NC)" -> EntercashField.Double,
  //    "Markup (NC)" -> EntercashField.String, //not found
  //    "Scheme Fees (NC)" -> EntercashField.String, //not found
  //    "Interchange (NC)" -> EntercashField.String, //not found
  //    "Payment Method Variant" -> EntercashField.String,
  //    "Modification Merchant Reference" -> EntercashField.Long,
  //    "Batch Number" -> EntercashField.Long, //can be Int ?
  //    "Reserved4" -> EntercashField.String, //not found
  //    "Reserved5" -> EntercashField.String, //not found
  //    "Reserved6" -> EntercashField.String, //not found
  //    "Reserved7" -> EntercashField.String, //not found
  //    "Reserved8" -> EntercashField.String, //not found
  //    "Reserved9" -> EntercashField.String, //not found
  //    "Reserved10" -> EntercashField.String, //not found
  //    "Reserved10" -> EntercashField.String, //not found
  //    "Shopper Reference" -> EntercashField.Long
  //  )
}

