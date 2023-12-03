package com.devcode.accountiq.settlement.recoonciliation

import java.time.{LocalDate, LocalDateTime}
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, EncoderOps, JsonDecoder, JsonEncoder}

final case class ReconTimeFrame(start: LocalDateTime, end: LocalDateTime) {

  def ++(other: ReconTimeFrame): ReconTimeFrame =
    ReconTimeFrame(
      if (start.isBefore(other.start)) start else other.start,
      if (end.isAfter(other.end)) end else other.end
    )

}

object ReconTimeFrame {

  def forDaysBack(daysBack: Int): ReconTimeFrame = {
    val now = LocalDate.now()
    ReconTimeFrame(now.minusDays(daysBack).atStartOfDay(), now.plusDays(1).atStartOfDay())
  }

  implicit val decoder: JsonDecoder[ReconTimeFrame] =
    DeriveJsonDecoder.gen[ReconTimeFrame]

  implicit val encoder: JsonEncoder[ReconTimeFrame] =
    DeriveJsonEncoder.gen[ReconTimeFrame]

}
