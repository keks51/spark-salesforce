package utils

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.stubFor
import com.github.tomakehurst.wiremock.client.{MappingBuilder, WireMock}
import com.github.tomakehurst.wiremock.stubbing.Scenario


trait MockedServer {

  def withMockedServer(host: String, port: Int)(f: WireMockServer => Unit): Unit = {
    val wireMockServer = new WireMockServer(port)
    wireMockServer.start()
    WireMock.configureFor(host, port)
    try { f(wireMockServer)} finally wireMockServer.stop()
  }

  def stubSeqOfScenarios(name: String)(seq: MappingBuilder*): Unit = {
    seq
      .map(_.inScenario(name))
      .zipWithIndex
      .map { case (req, i) =>
        if (i == 0)
          req.whenScenarioStateIs(Scenario.STARTED).willSetStateTo("1")
        else
          req.whenScenarioStateIs(i.toString).willSetStateTo((i + 1).toString)}
//      .map(stubFor)
      .zipWithIndex
//      .par
      .map { case (p, i) => println(s"Stubbing: '$i'"); stubFor(p) }
//      .seq
  }


}
