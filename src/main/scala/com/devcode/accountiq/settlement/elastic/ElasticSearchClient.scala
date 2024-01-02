package com.devcode.accountiq.settlement.elastic

import com.devcode.accountiq.settlement.{ElasticConfig, Remote}
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticNodeEndpoint, ElasticProperties, ElasticClient}
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import zio.ZLayer

object ElasticSearchClient {

  val live: ZLayer[ElasticConfig, Nothing, ElasticClient] = ZLayer.fromFunction(ElasticSearchClient.create _)

  def create(elasticConfig: ElasticConfig): ElasticClient = {
    elasticConfig.mode match {
      case "remote" =>
        makeRemoteClient(elasticConfig.remote)
      case "local" =>
        makeLocalClient(elasticConfig)
    }
  }

  private def makeLocalClient(elasticConfig: ElasticConfig) = {
    ElasticClient(JavaClient(ElasticProperties(
      options = Map.empty,
      endpoints = Seq(
        ElasticNodeEndpoint(
          "http",
          elasticConfig.local.host,
          elasticConfig.local.port,
          None
        )
      )
    )))
  }

  private def makeRemoteClient(remote: Remote) = {
    import remote._

    val credentialsProvider = new BasicCredentialsProvider
    credentialsProvider.setCredentials(
      AuthScope.ANY,
      new UsernamePasswordCredentials(
        username,
        password
      )
    )

    ElasticClient(JavaClient(
      props = ElasticProperties(url),
      requestConfigCallback = (requestConfigBuilder: RequestConfig.Builder) => requestConfigBuilder.setConnectionRequestTimeout(-1),
      httpClientConfigCallback = (httpClientBuilder: HttpAsyncClientBuilder) => httpClientBuilder
        .setDefaultCredentialsProvider(credentialsProvider)
        .setMaxConnTotal(maxConnTotal)
        .setMaxConnPerRoute(maxConnPerRoute)
    ))
  }
}
