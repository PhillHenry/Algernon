package uk.co.odinconsultants.algernon.matrix

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

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

}
