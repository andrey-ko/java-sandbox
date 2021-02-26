package org.andreyko.sandbox.projects.jackson.xml

import com.fasterxml.jackson.dataformat.xml.*

object ParseTree {
  
  @JvmStatic
  fun main(vararg args: String) {
    val xml = XmlMapper()
    val tree = this::class.java.getResourceAsStream("metadata.xml")!!.use { istream ->
      xml.readTree(istream)
    }
    val instanceId = tree.at("/RDF/Description/InstanceID").textValue()
    require(instanceId == "b655fa66-0b31-441a-82a7-95b322e6cdd1")
  }
}
