package com.devcode.accountiq.settlement.elastic.reports

import com.sksamuel.elastic4s.ElasticApi.objectField
import com.sksamuel.elastic4s.fields.{ElasticField, ObjectField}
import com.sksamuel.elastic4s.ElasticDsl.{intField, keywordField}

object Mappings {

  implicit final class ObjectFieldOps(val objectField: ObjectField) extends AnyVal {
    def fields(fields: ElasticField*): ObjectField = objectField.copy(properties = fields)
  }

  def moneyField(name: String): ObjectField =
    objectField(name).fields(
      intField("value"),
      keywordField("cy")
    )

}
