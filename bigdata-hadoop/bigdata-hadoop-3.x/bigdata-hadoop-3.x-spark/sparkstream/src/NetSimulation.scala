

import java.io.{ PrintWriter }
import java.net.ServerSocket
import scala.io.Source

object SaleSimulation {
  def randomIndex(length: Int) = {
    import java.util.Random
    val rdm = new Random
    rdm.nextInt(length)
  }

  def main(args: Array[String]): Unit = {

    val filename = "/usr/local/spark/README.md"
    val lines = Source.fromFile(filename).getLines.toList
    val filerow = lines.length

    val serverSocket = new ServerSocket(9999)
    while (true) {
      val socket = serverSocket.accept()
      new Thread() {
        override def run = {
          println("Got client connected from :" + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
          while (true) {
            Thread.sleep(1000)
            val content = lines(randomIndex(filerow))
            println(content)
            out.write(content + "\n")
            out.flush()
          }
          socket.close()
        }
      }.start()
    }

  }

}