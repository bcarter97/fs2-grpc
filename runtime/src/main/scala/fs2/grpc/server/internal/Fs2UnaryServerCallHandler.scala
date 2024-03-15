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

package fs2.grpc.server.internal

import cats.syntax.all.*
import cats.effect.std.Dispatcher
import cats.effect.{Async, Ref, Sync}
import fs2.grpc.server.{ServerCallOptions, ServerOptions}
import fs2.grpc.shared.StreamOutput
import io.grpc._

private[server] object Fs2UnaryServerCallHandler {

  import Fs2ServerCall.Cancel

  sealed trait CallerState[F[_], A]
  object CallerState {
    def init[F[_]: Sync, A](cb: A => F[Cancel]): F[Ref[F, CallerState[F, A]]] =
      Ref[F].of[CallerState[F, A]](PendingMessage(cb))
  }
  case class PendingMessage[F[_], A](callback: A => F[Cancel]) extends CallerState[F, A] {
    def receive(a: A): PendingHalfClose[F, A] = PendingHalfClose(callback, a)
  }
  case class PendingHalfClose[F[_], A](callback: A => F[Cancel], received: A) extends CallerState[F, A] {
    def call(): Called[F, A] = Called(callback(received))
  }
  case class Called[F[_], A](cancel: F[Cancel]) extends CallerState[F, A]
  case class Cancelled[F[_], A]() extends CallerState[F, A]

  private def mkListener[F[_]: Sync, Request, Response](
      dispatcher: Dispatcher[F],
      call: Fs2ServerCall[Request, Response],
      signalReadiness: F[Unit],
      state: Ref[F, CallerState[F, Request]]
  ): ServerCall.Listener[Request] =
    new ServerCall.Listener[Request] {
      override def onCancel(): Unit =
        dispatcher.unsafeRunSync(
          state.get
            .flatMap {
              case Called(cancel) =>
                cancel >> state.set(Cancelled[F, Request]())
              case _ => Sync[F].unit
            }
        )

      override def onMessage(message: Request): Unit = {
        dispatcher.unsafeRunSync(
          state.get
            .flatMap {
              case s: PendingMessage[F, Request] =>
                state.set(s.receive(message))
              case _: PendingHalfClose[F, Request] =>
                sendError(Status.INTERNAL.withDescription("Too many requests"))
              case _ =>
                Sync[F].unit
            }
        )
      }

      override def onHalfClose(): Unit = {
        dispatcher.unsafeRunSync(
          state.get
            .flatMap {
              case s: PendingHalfClose[F, Request] =>
                state.set(s.call())

              case _: PendingMessage[F, Request] =>
                sendError(Status.INTERNAL.withDescription("Half-closed without a request"))

              case _ =>
                Sync[F].unit
            }
        )
      }

      override def onReady(): Unit = dispatcher.unsafeRunSync(signalReadiness)

      private def sendError(status: Status): F[Unit] =
        state.set(Cancelled()) >> call.close(status, new Metadata())
    }

  def unary[F[_]: Async, Request, Response](
      impl: (Request, Metadata) => F[Response],
      options: ServerOptions,
      dispatcher: Dispatcher[F]
  ): ServerCallHandler[Request, Response] =
    new ServerCallHandler[Request, Response] {
      private val opt = options.callOptionsFn(ServerCallOptions.default)

      override def startCall(call: ServerCall[Request, Response], headers: Metadata): ServerCall.Listener[Request] =
        dispatcher.unsafeRunSync(
          startCallSync(dispatcher, call, Sync[F].unit, opt)(call => req => call.unary(impl(req, headers), dispatcher))
        )
    }

  def stream[F[_]: Async, Request, Response](
      impl: (Request, Metadata) => fs2.Stream[F, Response],
      options: ServerOptions,
      dispatcher: Dispatcher[F]
  ): ServerCallHandler[Request, Response] =
    new ServerCallHandler[Request, Response] {
      private val opt = options.callOptionsFn(ServerCallOptions.default)

      def startCall(call: ServerCall[Request, Response], headers: Metadata): ServerCall.Listener[Request] = {
        dispatcher.unsafeRunSync(
          StreamOutput.server(call).flatMap { outputStream =>
            startCallSync(dispatcher, call, outputStream.onReady, opt) { call => req =>
              call.stream(outputStream.writeStream, impl(req, headers), dispatcher)
            }
          }
        )
      }
    }

  private def startCallSync[F[_]: Sync, Request, Response](
      dispatcher: Dispatcher[F],
      call: ServerCall[Request, Response],
      signalReadiness: F[Unit],
      options: ServerCallOptions
  )(f: Fs2ServerCall[Request, Response] => Request => F[Cancel]): F[ServerCall.Listener[Request]] = {
    for {
      call <- Fs2ServerCall.setup(options, call)
      // We expect only 1 request, but we ask for 2 requests here so that if a misbehaving client
      // sends more than 1 requests, ServerCall will catch it.
      _ <- call.request(2)
      state <- CallerState.init(f(call))
    } yield mkListener[F, Request, Response](dispatcher, call, signalReadiness, state)
  }
}
