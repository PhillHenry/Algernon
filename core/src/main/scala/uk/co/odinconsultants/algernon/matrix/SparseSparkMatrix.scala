package uk.co.odinconsultants.algernon.matrix

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe.TypeTag

case class MatrixCell[T](i: Long, j: Long, x: T)

object SparseSparkMatrix {

  type SparseSpark[T] = Dataset[MatrixCell[T]]
  type CellPairOp[T]  = ((MatrixCell[T], MatrixCell[T])) => MatrixCell[T]
  type CellOp[T]      = (MatrixCell[T], MatrixCell[T]) => MatrixCell[T]

  implicit class SparseSparkOps[T: Encoder: TypeTag: Numeric](ds: SparseSpark[T]) {

    def multiply(other: SparseSpark[T])(implicit session: SparkSession): SparseSpark[T] = {
      import session.implicits._
      val mathOps                   = implicitly[Numeric[T]]
      val multiplied: CellPairOp[T] = { case (x, y) => MatrixCell(x.i, y.j, mathOps.times(x.x, y.x) ) }
      val added:      CellOp[T]     = { case (x, y) => x.copy(x = mathOps.plus(x.x, y.x)) }

      ds.joinWith(other, ds("j") === other("i"), "inner").map(multiplied).groupByKey(c => (c.i, c.j)).reduceGroups(added).map(_._2)
    }

    def transpose(implicit session: SparkSession): SparseSpark[T] = {
      import session.implicits._

      val transposing: MatrixCell[T] => MatrixCell[T] = x => x.copy(x.j, x.i, x.x)
      ds.map(transposing)
    }

    def frobeniusNormSquared(implicit session: SparkSession): T = {
      import session.sqlContext.implicits._
      val mathOps = implicitly[Numeric[T]]
      val squared = ds.select(col("x")).map(x => mathOps.times(x.getAs[T](0), x.getAs[T](0))).agg(sum(col("value"))).collect()(0).getAs[T](0)
      squared
    }
  }

}
