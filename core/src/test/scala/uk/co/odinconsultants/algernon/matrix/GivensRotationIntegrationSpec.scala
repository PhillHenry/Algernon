package uk.co.odinconsultants.algernon.matrix

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class GivensRotationIntegrationSpec extends WordSpec with Matchers {

  import MatrixMaker._
  import SparkForTesting.session.implicits._
  import SparseSparkMatrix._

  private implicit val session: SparkSession = SparkForTesting.session

  "Givens factorization" should {
    val width   = 5
    val height  = 5
    val density = 0.2
    val factor  = 100d
    val cells   = quasiRandomSparseMatrix(width, height, density, factor)
    val matrix  = toSparseMatrix(cells)

    "produce an upper triangular matrix" in {
      val rotationInfo = matrix.givensIndexes.orderBy('_1, '_2).collect()
      var n = 0d
      println("PH: Start with\n" + asString(cells) + "\n")
      val R = rotationInfo.foldLeft(matrix) { case(acc, (i, j, c, s)) =>
        println(s"PH: i = $i, j = $j, c = $c, s = $s ${(n / rotationInfo.length) * 100}% done")
        val newDS = matrix.rotateTo0(i, j, c, s, acc).persist()

        println("PH - Matrix:\n" + asString(newDS.collect()) + "\n\n") // seems to speed things up compared to matrix.givensRotation. Not sure why.

        n = n + 1
        newDS
      }

//      val R = matrix.givensRotation
      R.filter(lowerTriangular).collect() shouldBe empty
    }

    "first get the indices which are relevant" in {
      val actual = matrix.givensIndexes.collect()
      actual.foreach(x => println("x = " + x))
      actual.length shouldBe cells.count(lowerTriangular)
    }
  }

}
