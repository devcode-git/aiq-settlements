package com.devcode.accountiq.settlement.recoonciliation

final case class ReconcileCmd(
                               timeFrame: ReconTimeFrame,
                               merchant: String,
                               provider: String)
