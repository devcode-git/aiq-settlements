package com.devcode.accountiq.settlement.elastic.reports

import com.sksamuel.elastic4s.Indexable
import zio.json.EncoderOps

case class ESDoc(doc: Map[String, String])

object ESDoc {
  implicit val formatter: Indexable[ESDoc] = (t: ESDoc) => t.doc.toJson

  def parseESDocs(rows: List[List[String]], fileId: String, provider: String, merchant: String): List[ESDoc] = {
    rows match {
      case _ :: Nil => List()
      case Nil => List()
      case headerRow :: dataRows => dataRows.map(row => headerRow.zip(row)).map(_.toMap)
        .map(_ + (AIQField.filename.toString -> fileId))
        .map(_ + (AIQField.provider.toString -> provider))
        .map(_ + (AIQField.merchant.toString -> merchant))
        .map(ESDoc(_))
    }
  }
}
