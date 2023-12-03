package com.devcode.accountiq.settlement.recoonciliation

final case class ReconcileCmd(
                               timeFrame: ReconTimeFrame,
                               merchantId: Option[String] = None,
                               providerId: Option[String] = None)
