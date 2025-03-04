# About Streamiz

[Streamiz](https://github.com/LGouellec/streamiz) is a .NET stream processing library for Apache Kafka. As Kafka Streams does, Streamiz is easily installed as a dependency in your project.

# Install dotnet 8

https://dotnet.microsoft.com/en-us/download

# Configure environment variables

`KAFKA_BOOTSTRAP_SERVERS` = Boostrap server of your kafka cluster

`KAFKA_API_KEY` = API Key related to your Confluent Cloud cluster

`KAFKA_API_SECRET` = API Secret related to your Confluent Cloud cluster

`SCHEMA_REGISTRY_URL` = Schema Registry to your Confluent Cloud environment

`SCHEMA_REGISTRY_API_KEY` = Schema Registry API Key

`SCHEMA_REGISTRY_API_SECRET` = Schema Registry API Secret

# Run the application

```
cd Streamiz/
dotnet run
```