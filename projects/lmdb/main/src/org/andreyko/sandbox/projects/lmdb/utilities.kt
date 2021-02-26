package org.andreyko.sandbox.projects.lmdb

import java.nio.*
import java.nio.charset.*

val Long.kb get() = 1024L * this
val Long.mb get() = 1024L * kb
val Long.gb get() = 1024L * mb

val Int.kb get() = 1024 * this
val Int.mb get() = 1024 * kb
val Int.gb get() = 1024 * mb

fun ByteBuffer.putString(value: String, enc: CharsetEncoder) {
  var res = enc.encode(CharBuffer.wrap(value), this, true)
  if (res.isError) res.throwException()
  res = enc.flush(this)
  if (res.isError) res.throwException()
}