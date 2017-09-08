package uk.co.odinconsultants.algernon.matrix

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.algernon.matrix.MatrixMaker._

@RunWith(classOf[JUnitRunner])
class SparseSparkMatrixSpec extends WordSpec with Matchers {

  import SparseSparkMatrix._

  "The value of a cell" should {
    val i = 1
    val j = 2
    val x = 3
    "be returned if present" in {
      val as = List(MatrixCell(i, j, x))
      getOr0(as, i, j) shouldBe x
    }
    "be zero if absent" in {
      getOr0(Seq.empty[MatrixCell[Int]], i, j) shouldBe 0
    }
  }

  "values smaller than epsilons" should {
    "be ignored" in {
      val t = 0.1
      essentiallyZero(t) shouldBe false
      essentiallyZero(DoubleMaths.epsilon(1E-7)) shouldBe true
      essentiallyZero(DoubleMaths.epsilon(1E-8)) shouldBe true
    }
  }

  "Corner values" should {
    "be extracted" in {
      val cells = asCells(
        """1 2 3
          |4 5 6
          |7 8  9""".stripMargin, toNumeric)
      cells.filter(cornerValues(2, 0)).map(_.x) should contain allOf(1, 7, 3, 9)
    }
  }

}
