package configuration

import com.typesafe.config.{Config, ConfigFactory}

object Configurations
{
    private val config: Config = ConfigFactory.load()
    val AWS_ACCESS_KEY: String = config.getString("aws.access-key-id")
    val SECRET_ACCESS_KEY: String = config.getString("aws.secret-access-key")
    val REGION: String = config.getString("aws.region")
}
