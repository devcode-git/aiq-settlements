package com.devcode.accountiq.settlement.elastic.reports.batch

import com.devcode.accountiq.settlement.elastic.ESDoc
import com.devcode.accountiq.settlement.transformer.AIQParserUtil

import java.util.Date

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

case class BatchSalesToPayoutPaidOutReportRow(status: String,
                                              sales: Double,
                                              refunds: Double,
                                              salesRefund: Double,
                                              pending: Long,
                                              payoutDate: Date,
                                              paymentMethod: String,
                                              paymentMethodDescription: String,
                                              salesCount: Long,
                                              refundCount: Long) extends BatchSalesToPayoutReportRow

object BatchSalesToPayoutReport {
  def fromESDocRaw(esDoc: ESDoc): BatchSalesToPayoutReportRow = {
    val doc = esDoc.doc
    doc(BatchSalesToPayoutReportField.status) match {
      case status if status == "summary" =>
        BatchSalesToPayoutReportSummaryRow(
          status,
          doc(BatchSalesToPayoutReportField.sales).toDouble,
          doc(BatchSalesToPayoutReportField.refunds).toDouble,
          doc(BatchSalesToPayoutReportField.salesRefund).toDouble,
          doc(BatchSalesToPayoutReportField.pending).toLong,
          Option(doc(BatchSalesToPayoutReportField.payoutDate)).filter(_.nonEmpty).map(AIQParserUtil.dateFormat.parse),
          Option(doc(BatchSalesToPayoutReportField.paymentMethod)).filter(_.nonEmpty),
          Option(doc(BatchSalesToPayoutReportField.paymentMethodDescription)).filter(_.nonEmpty),
          doc(BatchSalesToPayoutReportField.salesCount).toLong,
          doc(BatchSalesToPayoutReportField.refundCount).toLong
        )
      case status if status == "paidOut" =>
        BatchSalesToPayoutPaidOutReportRow(
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
