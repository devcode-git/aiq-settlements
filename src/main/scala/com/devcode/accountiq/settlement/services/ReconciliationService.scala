package com.devcode.accountiq.settlement.services

import com.devcode.accountiq.settlement.elastic.reports.merchant.MerchantPaymentTransactionsReportRow
import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReportRow

object ReconciliationService {

  def reconcileMerchantReports(merchantReports: List[MerchantPaymentTransactionsReportRow],
                               settlementDetailReport: List[SettlementDetailReportRow]) = {
    merchantReports.partition { merchantReport =>
      settlementDetailReport.find {
        r => r.merchantReference == merchantReport.txRef
      }.filter { r =>
        //todo: check gross debit ?
        // merchantReport.amount is "10 EUR" or "23.75 RON"
        // settlement.grossCredit is 30 (usually is EURO) guess all adyen is eur
        // remove .toLong and use separate column
        r.grossCredit == merchantReport.amount.toLong
      }

    }
    ???

  }

}
