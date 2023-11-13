package com.devcode.accountiq.settlement.elastic

import com.devcode.accountiq.settlement.transformer.AIQParserUtil

import java.util.Date

object BatchSalesToPayoutReportField extends Enumeration {
  type BatchSalesToPayoutReportField = Value
  val status = Value("Status")
  val sales = Value("Sales")
  val refunds = Value("Refunds")
  val salesRefund = Value("Sales - Refunds")
  val pending = Value("Pending")
  val payoutDate = Value("Payout Date")
  val paymentMethod = Value("Payment Method")
  val paymentMethodDescription = Value("Payment Method Description")
  val salesCount = Value("Sales Count")
  val refundCount = Value("Refund Count")

  implicit def fieldToString(field: BatchSalesToPayoutReportField): String = field.toString
}

object BatchSalesToPayoutReport {
  def fromESDocRaw(esDoc: ESDoc): BatchSalesToPayoutReportRow = {
    val doc = esDoc.doc
    doc(BatchSalesToPayoutReportField.status) match {
      case "summary" =>
        BatchSalesToPayoutReportSummaryRow(
          doc(BatchSalesToPayoutReportField.status),
          doc(BatchSalesToPayoutReportField.sales).toDouble,
          doc(BatchSalesToPayoutReportField.refunds).toDouble,
          doc(BatchSalesToPayoutReportField.salesRefund).toDouble,
          doc(BatchSalesToPayoutReportField.pending).toLong,
          Option(doc(BatchSalesToPayoutReportField.payoutDate)).filter(_.nonEmpty).map( AIQParserUtil.dateFormat.parse),
          Option(doc(BatchSalesToPayoutReportField.paymentMethod)).filter(_.nonEmpty),
          Option(doc(BatchSalesToPayoutReportField.paymentMethodDescription)).filter(_.nonEmpty),
          doc(BatchSalesToPayoutReportField.salesCount).toLong,
          doc(BatchSalesToPayoutReportField.refundCount).toLong
        )
      case status =>
        BatchSalesToPayoutPaidOutReport(
          status,
          doc(BatchSalesToPayoutReportField.sales).toDouble,
          doc(BatchSalesToPayoutReportField.refunds).toDouble,
          doc(BatchSalesToPayoutReportField.salesRefund).toDouble,
          doc(BatchSalesToPayoutReportField.pending).toLong,
          AIQParserUtil.dateFormat.parse(doc(BatchSalesToPayoutReportField.payoutDate)),
          doc(BatchSalesToPayoutReportField.paymentMethod),
          doc(BatchSalesToPayoutReportField.paymentMethodDescription),
          doc(BatchSalesToPayoutReportField.salesCount).toLong,
          doc(BatchSalesToPayoutReportField.refundCount).toLong
        )
    }

  }
}

trait BatchSalesToPayoutReportRow
case class BatchSalesToPayoutReportSummaryRow(status: String,
                                    sales: Double,
                                    refunds: Double,
                                    salesRefund: Double,
                                    pending: Long,
                                    payoutDate: Option[Date],
                                    paymentMethod: Option[String],
                                    paymentMethodDescription: Option[String],
                                    salesCount: Long,
                                    refundCount: Long) extends BatchSalesToPayoutReportRow

case class BatchSalesToPayoutPaidOutReport(status: String,
                                  sales: Double,
                                  refunds: Double,
                                  salesRefund: Double,
                                  pending: Long,
                                  payoutDate: Date,
                                  paymentMethod: String,
                                  paymentMethodDescription: String,
                                  salesCount: Long,
                                  refundCount: Long) extends BatchSalesToPayoutReportRow

//  val batchSalesToPayoutReportMapping: Map[String, EntercashField.Value] = Map(
//    "Status" -> EntercashField.String,
//    "Sales" -> EntercashField.Long,
//    "Refunds" -> EntercashField.Double,
//    "Sales - Refunds" -> EntercashField.Long,
//    "Pending" -> EntercashField.Long, //might be double, always zero
//    "Payout Date" -> EntercashField.Date, //example: 03/08/2023
//    "Payment Method" -> EntercashField.String,
//    "Payment Method Description" -> EntercashField.String,
//    "Sales Count" -> EntercashField.Long,
//    "Refund Count" -> EntercashField.Long,
//  )

