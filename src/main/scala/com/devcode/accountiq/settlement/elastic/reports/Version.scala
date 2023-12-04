package com.devcode.accountiq.settlement.elastic.reports

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

case class Version(_seq_no: Long, _primary_term: Long)

object Version {
  implicit val decoder: JsonDecoder[Version] =
    DeriveJsonDecoder.gen[Version]

  implicit val encoder: JsonEncoder[Version] =
    DeriveJsonEncoder.gen[Version]
}