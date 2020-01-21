import cats.effect.ExitCase._
import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import fs2.concurrent.SignallingRef
import fs2.{INothing, Stream}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

// @formatter:off
/** Sample FS2 streams app.
  *
  * Expected workflow is as follows:
  * 1. Initial state received
  * 2. Markers streamed from the offset stored in initial state
  * 3. Markers update the state
  * 4. New state passed back to step 2 and are used to stream new markers
  * 5. At the same time all produced states (+ initial) are piped to [[Fs2Stream#snapshot]]
  *    and [[Fs2Stream#ping]] separately for additional processing
  * 6. In case of errors - the processing is retried with exponential backoff
  *
  * Unresolved questions:
  * 1. Is [[SignallingRef]] approach for passing states optimal?
  * 2. How to reset retry delays?
  */
// @formatter:on
object Fs2Stream extends IOApp {

  private val ShutdownTimeout = 1 minute
  private val SnapshotInterval = 5
  private val MarkersPerQuery = 4

  private val StateUpdateDelay = 2 seconds
  private val StateSize = 7

  private val PingLimit = 3
  private val PingInterval = 10 seconds
  private val PingsSendInterval = 100 millis

  private case class State(last: Option[Int] = None, nums: Set[Int] = Set.empty)

  def run(args: List[String]): IO[ExitCode] = {
    val stream = for {
      log <- Stream.eval(Slf4jLogger.fromClass[IO](this.getClass))
      result <- buildStream(log)
    } yield result
    stream.interruptWhen(Stream.awakeEvery[IO](ShutdownTimeout).map(_ => true))
      .compile.drain.as(ExitCode.Success)
  }

  private def buildStream(
    log: Logger[IO],
  ): Stream[IO, INothing] = {
    val states = for {
      _ <- Stream.eval(log.info("Hello world!"))
      sigState <- initialState(log) // TODO is [[SignallingRef]] optimal solution?
      state <- {
        (sigState.continuous flatMap { state =>
          markers(log, offset = state.last)
            .mapAccumulate(state) { (s, o) =>
              val num = o % StateSize
              val newNums = if (s.nums contains num) s.nums - num else s.nums + num
              val newState = s.copy(last = o.some, newNums)
              (newState, newState)
            }
            .map(_._1)
            .map {
              if (Random.nextInt(3) == 0)
                throw new Exception("Something failed!")
              else
                _
            }
            .evalTap { s =>
              sigState.set(s) *>
                log.info(s"State updated: $s") *>
                IO.sleep(StateUpdateDelay)
            }
        }).through(retryOnErrors(log))
          .concurrently(snapshot(log, sigState))
          .concurrently(ping(log, sigState))
      }
    } yield state

    states.drain
      .onFinalizeCase {
        case Completed =>
          log.info("Stream completed")

        case Canceled =>
          log.warn("Stream cancelled")

        case Error(t) =>
          log.error(s"Stream closed due to failure: ${ t.getMessage }")
      }
  }

  private def retryOnErrors[A](log: Logger[IO]) = (st: Stream[IO, A]) => {
    val nextDelay = (_: FiniteDuration) * 2
    val delays = Stream.unfold(1 second)(d => Some(d -> nextDelay(d))).covary[IO]
    val retriable = scala.util.control.NonFatal.apply _
    val maxAttempts = 3

    // FIXME delays should be reset after successfull call
    st.attempts(delays)
      .takeWhile(_.fold(err => retriable(err), _ => true))
      .mapAccumulate(maxAttempts) { case (attemptsLeft, o) =>
        o.fold(
          t => (attemptsLeft - 1, t.asLeft),
          s => (maxAttempts, s.asRight)
        )
      }
      .takeThrough(_._1 > 0)
      .evalFilter {
        case (attemptsLeft, Left(t)) if attemptsLeft > 0 =>
          log.error(s"Stream failure: ${ t.getMessage }") *>
            IO.pure(false)

        case _ =>
          IO.pure(true)
      }
      .map(_._2)
      .rethrow
  }

  private def initialState(log: Logger[IO]): Stream[IO, SignallingRef[IO, State]] = {
    Stream.emit(State(last = 1.some, nums = Set(1)))
      .evalMap { state =>
        log.info(s"Initial state received: $state") *>
          SignallingRef[IO, State](state)
      }
  }

  private def markers(log: Logger[IO], offset: Option[Int]) = {
    val off = offset.getOrElse(0)
    Stream.emits(off + 1 to off + MarkersPerQuery)
      .evalTap(m => log.info(s"Mark received: $m"))
  }

  private def snapshot(log: Logger[IO], sigState: SignallingRef[IO, State]) = {
    sigState.discrete
      .zipWithIndex
      .collect { case (state, idx) if idx > 0 && idx % SnapshotInterval == 0 => state }
      .evalTap(state => log.info(s"State snapshot made: $state"))
  }

  private def ping(log: Logger[IO], sigState: SignallingRef[IO, State]) = {
    Stream.repeatEval(sigState.get)
      .metered(PingInterval)
      .flatMap(state => Stream.emits(state.nums.grouped(PingLimit).toSeq))
      .metered(PingsSendInterval)
      .evalTap { nums =>
        if (nums.isEmpty)
          log.info(s"Ping wasn't sent, state is empty")
        else
          log.info(s"Ping sent for: $nums")
      }
  }
}
