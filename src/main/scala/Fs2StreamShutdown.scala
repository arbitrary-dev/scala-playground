import cats.effect.ExitCase._
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import fs2.Stream
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

/** Fs2 stream app to express async stream shutdown */
object Fs2StreamShutdown extends App {

  private implicit val timer: Timer[IO] = IO.timer(implicitly)
  private implicit val cs: ContextShift[IO] = IO.contextShift(implicitly)
  private implicit val conc: Concurrent[IO] = IO.ioConcurrentEffect

  val stream = for {
    log <- Stream.eval(Slf4jLogger.fromClass[IO](this.getClass))
    result <- Stream.repeatEval(log.info("Hello world!")).metered(1 second)
      .onFinalizeCase {
        case Completed =>
          log.warn("Stream completed")

        case Canceled =>
          log.warn("Stream cancelled")

        case Error(t) =>
          log.error(s"Stream failure: ${ t.getMessage }")
      }
  } yield result

  val fib = stream.spawn.compile.lastOrError.unsafeRunSync()

  Thread.sleep(5000) // 5 seconds

  println("Stop it!")
  fib.cancel.unsafeRunSync()

  Thread.sleep(5000) // 5 seconds
}
