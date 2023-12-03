package com.devcode.accountiq.settlement.elastic.reports

object AIQField extends Enumeration {
  type AIQField = Value
  val filename = Value("aiqFilename")
  val provider = Value("aiqProvider")
  val merchant = Value("aiqMerchant")

  implicit def fieldToString(field: AIQField): String = field.toString
}
