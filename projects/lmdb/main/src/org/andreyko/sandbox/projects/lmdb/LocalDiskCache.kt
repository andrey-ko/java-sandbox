package org.andreyko.sandbox.projects.lmdb

import org.lmdbjava.*
import java.io.*
import java.nio.*
import java.nio.charset.*

object LocalDiskCache {
  val path: File = File(".data/lmdb")
  val maxSize: Long = 200L.mb
  val env: Env<ByteBuffer>
  val tbl: Dbi<ByteBuffer>
  var keyBuf: ByteBuffer?
  var valBuf: ByteBuffer?
  var charBuf: CharBuffer?
  
  var charEnc: CharsetEncoder?
  var charDec: CharsetDecoder?
  
  init {
    path.mkdirs()
    env = Env.create()
      .setMapSize(maxSize)
      .setMaxDbs(1)
      .open(path)
    
    tbl = env.openDbi("cache", DbiFlags.MDB_CREATE)
    
    keyBuf = ByteBuffer.allocateDirect(env.maxKeySize)
    valBuf = ByteBuffer.allocateDirect(4.mb)
    charBuf = CharBuffer.allocate(4.mb)
    
    charEnc = Charsets.UTF_8.newEncoder()
    charDec = Charsets.UTF_8.newDecoder()
  }
  
  @JvmStatic
  fun main(vararg args: String) {
    // ignore WARNING: An illegal reflective access operation has occurred
    // https://github.com/baremaps/baremaps/issues/165
    try {
      println("putting values")
      env.txnWrite().use {
        for (i in 1..1000_000) {
          set(it, "key-$i", "value-$i")
        }
        it.commit()
      }
      println("reading values")
      for (i in 1..1000_000) {
        require(get("key-$i") == "value-$i")
      }
    } finally {
      close()
    }
  }
  
  fun close() {
    env.sync(true)
    tbl.close()
    env.close()
  }
  
  fun set(key: String, value: String?, overwrite: Boolean = true) = env.txnWrite().use {
    set(it, key, value, overwrite)
    it.commit()
  }
  
  fun get(key: String): String? = env.txnRead().use {
    get(it, key)
  }
  
  fun set(txn: Txn<ByteBuffer>, key: String, value: String?, overwrite: Boolean = true) {
    if (value === null) {
      nodeKey(key) { tbl.delete(txn, it) }
      return
    }
    useValBuf { vbuf ->
      useCharEnc { vbuf.putString(value, it) }
      vbuf.flip()
      nodeKey(key) {
        if (overwrite) {
          if (!tbl.put(txn, it, vbuf)) throw Exception("put failed")
        } else {
          if (!tbl.put(txn, it, vbuf, PutFlags.MDB_NOOVERWRITE)) throw Exception("already exists")
        }
      }
    }
  }
  
  fun get(txn: Txn<ByteBuffer>, key: String): String? {
    useValBuf { vbuf ->
      val buf = nodeKey(key) { tbl.get(txn, it) } ?: return null
      return readString(buf)
    }
  }
  
  inline fun <reified T> useKeyBuf(block: (ByteBuffer) -> T): T {
    val v = keyBuf ?: throw Exception("buffer already in use")
    try {
      keyBuf = null
      v.clear()
      return block(v)
    } finally {
      keyBuf = v
    }
  }
  
  inline fun <reified T> useCharBuf(block: (CharBuffer) -> T): T {
    val v = charBuf ?: throw Exception("buffer already in use")
    try {
      charBuf = null
      v.clear()
      return block(v)
    } finally {
      charBuf = v
    }
  }
  
  fun readString(buf: ByteBuffer) = useCharBuf { cbuf ->
    useCharDec { dec ->
      var dres = dec.decode(buf, cbuf, true)
      if (dres.isError) dres.throwException()
      dres = dec.flush(cbuf)
      if (dres.isError) dres.throwException()
    }
    cbuf.flip()
    cbuf.toString()
  }
  
  inline fun <reified T> nodeKey(key: String, block: (ByteBuffer) -> T): T = useKeyBuf { buf ->
    useCharEnc { buf.putString(key, it) }
    buf.flip()
    block(buf)
  }
  
  inline fun <T> useValBuf(block: (ByteBuffer) -> T): T {
    val v = valBuf ?: throw Exception("buffer already in use")
    try {
      valBuf = null
      v.clear()
      return block(v)
    } finally {
      valBuf = v
    }
  }
  
  inline fun <reified T> useCharEnc(block: (CharsetEncoder) -> T): T {
    val v = charEnc ?: throw Exception("encoder already in use")
    try {
      charEnc = null
      v.reset()
      return block(v)
    } finally {
      charEnc = v
    }
  }
  
  inline fun <reified T> useCharDec(block: (CharsetDecoder) -> T): T {
    val v = charDec ?: throw Exception("decoder already in use")
    try {
      charDec = null
      v.reset()
      return block(v)
    } finally {
      charDec = v
    }
  }
}
