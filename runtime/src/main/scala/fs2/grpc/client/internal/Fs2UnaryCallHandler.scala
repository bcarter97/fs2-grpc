/*
 * Copyright (c) 2018 Gary Coady / Fs2 Grpc Developers
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fs2.grpc.client.internal

import cats.effect.kernel.{Async, Outcome, Ref}
import cats.effect.std.Dispatcher
import cats.effect.syntax.all._
import cats.effect.{Sync}
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.applicativeError._
import fs2._
import fs2.grpc.client.ClientOptions
import fs2.grpc.shared.StreamOutput
import io.grpc._

private[client] object Fs2UnaryCallHandler {
  sealed trait ReceiveState[R]

  object ReceiveState {
    def init[F[_]: Sync, R](
        callback: Either[Throwable, R] => Unit,
        pf: PartialFunction[StatusRuntimeException, Exception]
    ): F[Ref[F, ReceiveState[R]]] =
      Ref.in(new PendingMessage[F, R]({
        case r: Right[Throwable, R] => callback(r)
        case Left(e: StatusRuntimeException) => callback(Left(pf.lift(e).getOrElse(e)))
        case l: Left[Throwable, R] => callback(l)
      }))
  }

  class PendingMessage[F[_]: Sync, R](callback: Either[Throwable, R] => Unit) extends ReceiveState[R] {
    def receive(message: R): PendingHalfClose[F, R] = new PendingHalfClose(callback, message)

    def sendError(error: Throwable): F[ReceiveState[R]] =
      Sync[F].delay(callback(Left(error))).as(new Done[R])
  }

  class PendingHalfClose[F[_]: Sync, R](callback: Either[Throwable, R] => Unit, message: R) extends ReceiveState[R] {
    def sendError(error: Throwable): F[ReceiveState[R]] =
      Sync[F].delay(callback(Left(error))).as(new Done[R])

    def done: F[ReceiveState[R]] = Sync[F].delay(callback(Right(message))).as(new Done[R])
  }

  class Done[R] extends ReceiveState[R]

  private def mkListener[F[_]: Sync, Response](
      dispatcher: Dispatcher[F],
      state: Ref[F, ReceiveState[Response]],
      signalReadiness: F[Unit]
  ): ClientCall.Listener[Response] =
    new ClientCall.Listener[Response] {
      override def onMessage(message: Response): Unit = {
        dispatcher.unsafeRunSync(
          state.get
            .flatMap {
              case expected: PendingMessage[F, Response] =>
                state.set(expected.receive(message))

              case current: PendingHalfClose[F, Response] =>
                current
                  .sendError(
                    Status.INTERNAL
                      .withDescription("More than one value received for unary call")
                      .asRuntimeException()
                  )
                  .flatMap(state.set)

              case _ => Sync[F].unit
            }
        )
      }

      override def onClose(status: Status, trailers: Metadata): Unit = {
        dispatcher.unsafeRunSync(
          if (status.isOk) {
            state.get.flatMap {
              case expected: PendingHalfClose[F, Response] =>
                expected.done.flatMap(state.set)
              case current: PendingMessage[F, Response] =>
                current
                  .sendError(
                    Status.INTERNAL
                      .withDescription("No value received for unary call")
                      .asRuntimeException(trailers)
                  )
                  .flatMap(state.set)
              case _ => Sync[F].unit
            }
          } else {
            state.get.flatMap {
              case current: PendingHalfClose[F, Response] =>
                current.sendError(status.asRuntimeException(trailers)).flatMap(state.set)
              case current: PendingMessage[F, Response] =>
                current.sendError(status.asRuntimeException(trailers)).flatMap(state.set)
              case _ => Sync[F].unit
            }
          }
        )
      }

      override def onReady(): Unit = dispatcher.unsafeRunSync(signalReadiness)
    }

  def unary[F[_]: Async, Request, Response](
      dispatcher: Dispatcher[F],
      call: ClientCall[Request, Response],
      options: ClientOptions,
      message: Request,
      headers: Metadata
  ): F[Response] = Async[F].async[Response] { cb =>
    ReceiveState.init(cb, options.errorAdapter).map { state =>
      call.start(mkListener[F, Response](dispatcher, state, Sync[F].unit), headers)
      // Initially ask for two responses from flow-control so that if a misbehaving server
      // sends more than one responses, we can catch it and fail it in the listener.
      call.request(2)
      call.sendMessage(message)
      call.halfClose()
      Some(onCancel(call))
    }
  }

  def stream[F[_]: Async, Request, Response](
      call: ClientCall[Request, Response],
      options: ClientOptions,
      dispatcher: Dispatcher[F],
      messages: Stream[F, Request],
      output: StreamOutput[F, Request],
      headers: Metadata
  ): F[Response] = Async[F].async[Response] { cb =>
    ReceiveState.init(cb, options.errorAdapter).flatMap { state =>
      call.start(mkListener[F, Response](dispatcher, state, output.onReady), headers)
      // Initially ask for two responses from flow-control so that if a misbehaving server
      // sends more than one responses, we can catch it and fail it in the listener.
      call.request(2)
      output
        .writeStream(messages)
        .compile
        .drain
        .guaranteeCase {
          case Outcome.Succeeded(_) => Async[F].delay(call.halfClose())
          case Outcome.Errored(e) => Async[F].delay(call.cancel(e.getMessage, e))
          case Outcome.Canceled() => onCancel(call)
        }
        .handleError(_ => ())
        .start
        .map(sending => Some(sending.cancel >> onCancel(call)))
    }
  }

  private def onCancel[F[_]](call: ClientCall[_, _])(implicit F: Sync[F]): F[Unit] =
    F.delay(call.cancel("call was cancelled", null))

}
