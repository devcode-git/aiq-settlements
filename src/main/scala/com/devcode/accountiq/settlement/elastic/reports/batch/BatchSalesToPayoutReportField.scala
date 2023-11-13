package com.devcode.accountiq.settlement.elastic.reports.batch

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
