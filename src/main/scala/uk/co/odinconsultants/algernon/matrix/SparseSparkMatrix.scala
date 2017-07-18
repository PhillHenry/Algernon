package uk.co.odinconsultants.algernon.matrix

import org.apache.spark.sql.Dataset

case class MatrixCell[T](i: Long, j: Long, x: T) // TODO make x configurable

object SparseSparkMatrix {


//  type Cell[T <: Numeric[_]] = (Long, Long, T)
  type SparseSpark[T]   = Dataset[MatrixCell[T]]

}
