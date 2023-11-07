package com.devcode.accountiq.settlement.transformer

import com.devcode.accountiq.settlement.elastic.ESDoc

import java.nio.file.Path
import com.github.tototoshi.csv._
import zio.stream.ZStream

object CSVParser {

  def parse(path: Path, defaultDelimiter: Char = ',') = {
    implicit object csvFormat extends DefaultCSVFormat {
      override val delimiter: Char = defaultDelimiter
    }
    for {
      rows <- ZStream.fromIterator(CSVReader.open(path.toFile).iterator).map(_.toList).runCollect
    } yield rows.toList
  }

}
