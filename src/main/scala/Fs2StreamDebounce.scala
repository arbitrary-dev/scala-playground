import cats.effect.ExitCase._
import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

/** Sample FS2 streams debounce with initial element emitted right away */
object Fs2StreamDebounce extends IOApp {

  private val ShutdownTimeout = 10 seconds
  private val DebounceInterval = 1 second

  def run(args: List[String]): IO[ExitCode] = {
    val stream = for {
      log <- Stream.eval(Slf4jLogger.fromClass[IO](this.getClass))
      result <- buildStream(log)
        .onFinalizeCase {
          case Completed =>
            log.warn("Stream completed")

          case Canceled =>
            log.warn("Stream cancelled")

          case Error(t) =>
            log.error(s"Stream failure: ${ t.getMessage }")
        }
    } yield result

    stream.interruptAfter(ShutdownTimeout)
      .compile.drain.as(ExitCode.Success)
  }

  private def intStream(start: Int) =
    Stream.fromIterator[IO](scala.Stream.from(start).toIterator)

  private def bounce(log: Logger[IO])(i: Int) =
    log.info(s"Bounce $i")

  private def buildStream(log: Logger[IO]): Stream[IO, Int] = {
    Stream.eval(IO { Random.nextInt(10) })
      .evalTap { i =>
        log.info(s"Lets start with $i!") *>
        bounce(log)(i)
      }
      .flatMap { init =>
        intStream(init)
          .metered(500 millis)
          .debounce(DebounceInterval)
      }
      .evalTap(bounce(log))
  }
}
