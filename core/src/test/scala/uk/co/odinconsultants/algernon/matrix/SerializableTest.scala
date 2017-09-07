package uk.co.odinconsultants.algernon.matrix

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.algernon.matrix.SparseSparkMatrix.Maths

@RunWith(classOf[JUnitRunner])
class SerializableTest extends WordSpec with Matchers {

  "Functions" should {
    "be serializable" in {
      val maths = implicitly[Maths[Double]]
      val mathOps = implicitly[Numeric[Double]]
      val fn = SparseSparkMatrix.createCS(maths, mathOps)
      serialize(fn)
    }
  }

  "Implicits" should {
    "be serializable" in {
      val maths = implicitly[Maths[Double]]
      serialize(maths)
    }
  }

  "numerics" should {
    "be serializable" in {
      serialize(Numeric.DoubleAsIfIntegral)
    }
  }

  def serialize(x: Any): Unit = {
    val baos  = new ByteArrayOutputStream()
    val oos   = new ObjectOutputStream(baos)
    oos.writeObject(x)
  }

}
