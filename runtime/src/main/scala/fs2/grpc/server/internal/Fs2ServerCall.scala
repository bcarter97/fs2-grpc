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

import cats.effect.syntax.all.*
import cats.syntax.all._
import cats.effect.syntax.all._
import cats.effect._
import cats.effect.std.Dispatcher
import fs2._
import fs2.grpc.server.ServerCallOptions
import io.grpc._

private[server] object Fs2ServerCall {
  type Cancel = Unit

  def setup[F[_]: Sync, I, O](
      options: ServerCallOptions,
      call: ServerCall[I, O]
  ): F[Fs2ServerCall[I, O]] =
    Sync[F].delay {
      call.setMessageCompression(options.messageCompression)
      options.compressor.map(_.name).foreach(call.setCompression)
      new Fs2ServerCall[I, O](call)
    }
}

private[server] final class Fs2ServerCall[Request, Response](
    call: ServerCall[Request, Response]
) {

  import Fs2ServerCall.Cancel

  def stream[F[_]: Async](
      sendStream: Stream[F, Response] => Stream[F, Unit],
      response: Stream[F, Response],
      dispatcher: Dispatcher[F]
  ): F[Cancel] =
    run(
      response.pull.peek1
        .flatMap {
          case Some((_, stream)) =>
            Pull.suspend {
              call.sendHeaders(new Metadata())
              sendStream(stream).pull.echo
            }
          case None => Pull.done
        }
        .stream
        .compile
        .drain,
      dispatcher
    )

  def unary[F[_]: Async](response: F[Response], dispatcher: Dispatcher[F]): F[Cancel] = {
    run(
      response
        .flatMap { message =>
          Sync[F].delay {
            call.sendHeaders(new Metadata())
            call.sendMessage(message)
          }
        },
      dispatcher
    )
  }

  def request[F[_]: Sync](n: Int): F[Unit] =
    Sync[F].delay(call.request(n))

  def close[F[_]: Sync](status: Status, metadata: Metadata): F[Unit] =
    Sync[F].delay(call.close(status, metadata))

  private def run[F[_]: Async](completed: F[Unit], dispatcher: Dispatcher[F]): F[Cancel] = {
    completed
      .flatTap(_ => Sync[F].delay(println("Potentially poggers")))
      .guaranteeCase {
        case Outcome.Succeeded(_) => Sync[F].delay(println(">>>>>>>>>>>>>> Success")) >> close(Status.OK, new Metadata())
        case Outcome.Errored(e) => Sync[F].delay(println(s">>>>>>>>>>>> Error $e")) >> handleError(e)
        case Outcome.Canceled() => Sync[F].delay(println(">>>>>>>>>>>>>> Canceled")) >> close(Status.CANCELLED, new Metadata())
      }
      .handleError(_ => ())
      .start
      .void
  }

  private def handleError[F[_]: Sync](t: Throwable): F[Unit] = t match {
    case ex: StatusException => close(ex.getStatus, Option(ex.getTrailers).getOrElse(new Metadata()))
    case ex: StatusRuntimeException => close(ex.getStatus, Option(ex.getTrailers).getOrElse(new Metadata()))
    case ex => close(Status.INTERNAL.withDescription(ex.getMessage).withCause(ex), new Metadata())
  }
}
