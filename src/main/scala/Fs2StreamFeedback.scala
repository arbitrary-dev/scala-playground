import cats.effect.ExitCase._
import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import fs2.concurrent.{Signal, SignallingRef}
import fs2.{INothing, Pipe, Stream}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

// @formatter:off
/** Sample FS2 streams app with feedback.
  *
  * Expected workflow is as follows:
  * 1. Initial state received
  * 2. Markers streamed from the offset stored in initial state
  * 3. Markers update the state continiously
  * 4. Running state passed downstream to [[Fs2StreamFeedback#snapshots]]
  * 5. Errors cause a mark to be processed again with exponential backoff
  * 6. At the same time accumulated state is pinged with some time interval, see [[Fs2StreamFeedback#pings]]
  *
  * Unresolved questions:
  * 1. How to reset retry delays?
  */
// @formatter:on
object Fs2StreamFeedback extends IOApp {

  private val ShutdownTimeout = 1 minute
  private val SnapshotInterval = 5
  private val MarkersPerQuery = 4

  private val StateUpdateDelay = 2 seconds
  private val StateSize = 7

  private val PingLimit = 3
  private val PingInterval = 10 seconds
  private val PingsSendInterval = 100 millis

  private case class State(last: Option[Int] = None, nums: Set[Int] = Set.empty)

  def run(args: List[String]): IO[ExitCode] =
    Slf4jLogger.fromClass[IO](this.getClass) flatMap { implicit log =>
      nums.evalTap(ping)
        .interruptAfter(ShutdownTimeout)
        .compile.drain.as(ExitCode.Success)
    }

  private def nums(implicit log: Logger[IO]): Stream[IO, Set[Int]] = {
    val stream = for {
      _ <- Stream.eval(log.info("Hello world!"))
      sigState <- initialState
      snapshotting = states(sigState).through(snapshots)
      pingSet <- pings(sigState).concurrently(snapshotting)
    } yield pingSet

    stream.onFinalizeCase {
      case Completed =>
        log.info("Stream completed")

      case Canceled =>
        log.warn("Stream cancelled")

      case Error(t) =>
        log.error(s"Stream closed due to failure: ${ t.getMessage }")
    }
  }

  private def retryOnErrors[A](implicit log: Logger[IO]) = (st: Stream[IO, A]) => {
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

  private def initialState(implicit log: Logger[IO]): Stream[IO, SignallingRef[IO, State]] = {
    Stream.emit(State(last = 1.some, nums = Set(1)))
      .evalMap { state =>
        log.info(s"Initial state received: $state") *>
          SignallingRef[IO, State](state) // Provides feedback mechanism
      }
  }

  private def states(sigState: SignallingRef[IO, State])(implicit log: Logger[IO]) =
    (sigState.continuous flatMap { state =>
      markers(offset = state.last)
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
    }).through(retryOnErrors)

  private def markers(offset: Option[Int])(implicit log: Logger[IO]) = {
    val off = offset.getOrElse(0)
    Stream.emits(off + 1 to off + MarkersPerQuery)
      .evalTap(m => log.info(s"Mark received: $m"))
  }

  private def snapshots(implicit log: Logger[IO]): Pipe[IO, State, INothing] =
    _.zipWithIndex
      .collect { case (state, idx) if idx > 0 && idx % SnapshotInterval == 0 => state }
      .evalTap(state => log.info(s"State snapshot made: $state"))
      .drain

  private def pings(sigState: Signal[IO, State])(implicit log: Logger[IO]) =
    (Stream.eval(sigState.get) ++ sigState.continuous.metered(PingInterval))
      .flatMap { state =>
        Stream.emits(state.nums.grouped(PingLimit).toSeq)
          .covary[IO]
          .metered(PingsSendInterval)
      }

  private def ping(nums: Set[Int])(implicit log: Logger[IO]) =
    if (nums.isEmpty)
      log.info(s"Ping wasn't sent, state is empty")
    else
      log.info(s"Ping sent for: $nums")
}
