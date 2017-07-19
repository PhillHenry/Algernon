package uk.co.odinconsultants.algernon.matrix

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import scala.reflect.runtime.universe.TypeTag

case class MatrixCell[T](i: Long, j: Long, x: T) // TODO make x configurable

object SparseSparkMatrix {

  type SparseSpark[T]   = Dataset[MatrixCell[T]]

  implicit class SparseSparkOps[T: Encoder: TypeTag: Numeric](ds: SparseSpark[T]) {
    def multiply(other: SparseSpark[T])(implicit session: SparkSession): SparseSpark[T] = {
      import session.implicits._

      val mathOps = implicitly[Numeric[T]]

      val multiplied: ((MatrixCell[T], MatrixCell[T])) => MatrixCell[T] = { case (x, y) => MatrixCell(x.i, y.j, mathOps.times(x.x, y.x) ) }

      val added: (MatrixCell[T], MatrixCell[T]) => MatrixCell[T] = { case (x, y) => x.copy(x = mathOps.plus(x.x, y.x)) }

      ds.joinWith(other, ds("j") === other("i"), "inner").map(multiplied).groupByKey(c => (c.i, c.j)).reduceGroups(added).map(_._2)
    }
  }

}
