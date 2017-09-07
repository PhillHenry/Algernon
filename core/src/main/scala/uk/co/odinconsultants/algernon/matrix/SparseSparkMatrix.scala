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
      ds.select(col("x")).map(x => mathOps.times(x.getAs[T](0), x.getAs[T](0))).agg(sum(col("value"))).collect()(0).getAs[T](0)
    }

    def givensRotation(): SparseSpark[T] = {
      ???
    }

    def givensIndexes(implicit session: SparkSession, maths: Maths[T]): Dataset[(Long, Long, T, T)] = {
      import session.sqlContext.implicits._
      val lowerTriangle = ds.filter(lowerTriangular)
      val diagonal      = ds.filter(c => c.i == c.j)
      val aij_aii       = lowerTriangle.joinWith(diagonal, '_1("i") === '_2("i"), "left_outer")

      val mathOps = implicitly[Numeric[T]]
//      import SparseSparkMatrix._
//      val maths = implicitly[Maths[T]]

      aij_aii.map { case (aij, aii) =>
//        val (c, s) = csOf(aij, aii, mathOps) // this is giving serialization errors. don't know why yet. TODO

        import mathOps.mkNumericOps
        import maths._
        val aijx = if (aij == null) mathOps.zero else aij.x
        val aiix = if (aii == null) mathOps.zero else aii.x
        val r   = power(power(aijx, 2) + power(aiix, 2), 0.5)
        val c   = divide(aiix, r)
        val s   = divide(aijx, r)


        (aij.i, aij.j, c, s)
      }
    }

    /**
    G = np.eye(len(A))
    aji = A[j, i]
    aii = A[i, i]
    r = ((aji ** 2) + (aii ** 2)) ** 0.5
    c = aii / r
    s = - aji / r
    G[i, j] = -s
    G[i, i] = c
    G[j, j] = c
    G[j, i] = s
      */
    def make0(i: Long, j: Long)(implicit session: SparkSession, maths: Maths[T]): SparseSpark[T] = {
      import session.sqlContext.implicits._
      val as = ds.filter(c => (c.i == i || c.i == j) && (c.j == i || c.j == j)).collect()
      val aji = getOr0(as, i, j)
      val aii = getOr0(as, i, i)

      val mathOps = implicitly[Numeric[T]]
      val (c, s) = cs(aji, aii, mathOps, maths)

      val jthRow = ds.filter(_.i == j)
      val ithRow = ds.filter(_.i == i)
      val newRows = rotate(jthRow, ithRow, c, s)
      ds.filter(c => c.i != i && c.i != j).union(newRows)
    }

  }

  def csOf[T: Encoder : TypeTag : Numeric](aij: MatrixCell[T], aii: MatrixCell[T], mathOps: Numeric[T]): (T, T) = {
    val aijx = if (aij == null) mathOps.zero else aij.x
    val aiix = if (aii == null) mathOps.zero else aii.x
//    cs(aijx, aiix, mathOps, maths)
//    import mathOps.mkNumericOps
//    val r   = (aijx * aijx) + (aiix * aiix)
//    val c   = aiix * r
//    val s   = aijx * r
//    (c, s)
    (mathOps.zero, mathOps.zero)
  }

  def cs[T: Encoder : TypeTag : Numeric](aji: T, aii: T, mathOps: Numeric[T], maths: Maths[T]): (T, T) = {
    import mathOps.mkNumericOps
    import maths._
    val r   = power(power(aji, 2) + power(aii, 2), 0.5)
    val c   = divide(aii, r)
    val s   = divide(aji, r)
    (c, s)
  }

  def rotate[T: Encoder : TypeTag : Numeric](jthRow: SparseSpark[T], ithRow: SparseSpark[T], c: T, s: T)(implicit session: SparkSession): SparseSpark[T] = {
    val mathOps = implicitly[Numeric[T]]
    import session.sqlContext.implicits._
    import mathOps.mkNumericOps
    ithRow.joinWith(jthRow, '_1 ("j") === '_2 ("j"), "outer").flatMap { case (x, y) =>

      def calculate(x: MatrixCell[T], y: MatrixCell[T], _s: T) =
        {if (x == null) mathOps.zero else (x.x * c)} + {if (y == null) mathOps.zero else (y.x * _s)}

      val newIth = if (x == null) Seq() else Seq(MatrixCell(x.i, x.j, calculate(x, y, -s)))
      val newJth = if (y == null) Seq() else Seq(MatrixCell(y.i, y.j, calculate(x, y, s)))

      newIth ++ newJth
    }.filter(_.x != mathOps.zero)
  }

  def getOr0[T: Numeric](ts: Seq[MatrixCell[T]], i: Long, j: Long): T = {
    val mathOps = implicitly[Numeric[T]]
    val maybe   = ts.filter(c => c.i == i && c.j == j)
    maybe.headOption.map(_.x).getOrElse(mathOps.zero)
  }

  trait Maths[T] extends Serializable {
    def power(x: T, y: Double): T
    def divide(x: T, y: T): T
  }

  implicit val DoubleMaths: Maths[Double] = new Maths[Double] {
    override def power(x: Double, y: Double): Double = Math.pow(x, y)
    override def divide(x: Double, y: Double): Double = x / y
  }

  def lowerTriangular[_]: MatrixCell[_] => Boolean = { c => c.i > c.j }

}
