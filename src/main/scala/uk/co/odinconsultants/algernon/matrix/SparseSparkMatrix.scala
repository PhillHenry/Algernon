package uk.co.odinconsultants.algernon.matrix

import org.apache.spark.sql.Dataset

object SparseSparkMatrix {

  case class MatrixCell(i: Long, j: Long, x: Int) // TODO make x configurable

//  type Cell[T <: Numeric[_]] = (Long, Long, T)
  type SparseSpark   = Dataset[MatrixCell]

}
