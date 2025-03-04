using com.kafkaStreamsExample;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

static string GetEnvironmentVariableOrDefault(string key, string @default)
{
    var value = Environment.GetEnvironmentVariable(key);
    return value ?? @default;
}

var streamConfig = new StreamConfig<StringSerDes, StringSerDes>
{
    ApplicationId = "gko-csta-streamiz",
    BootstrapServers = GetEnvironmentVariableOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    SaslUsername = GetEnvironmentVariableOrDefault("KAFKA_API_KEY", ""),
    SaslPassword = GetEnvironmentVariableOrDefault("KAFKA_API_SECRET", ""),
    SaslMechanism = Confluent.Kafka.SaslMechanism.Plain,
    SecurityProtocol = Confluent.Kafka.SecurityProtocol.SaslSsl,
    SchemaRegistryUrl = GetEnvironmentVariableOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
    BasicAuthCredentialsSource = "USER_INFO",
    BasicAuthUserInfo =
        $"{GetEnvironmentVariableOrDefault("SCHEMA_REGISTRY_API_KEY", "")}:{GetEnvironmentVariableOrDefault("SCHEMA_REGISTRY_API_SECRET", "")}",
};

var windowSizeInMillis = 10000L;

SensorData ConvertTemperature(SensorData raw)
{
    raw.value = raw.value.Select(v =>
    {
        if (v.type == "temperature" && v.unit == "Fahrenheit")
        {
            v.value = (v.value - 32) / 1.8;
            v.unit = "Celsius";
        }

        return v;
    }).ToList();
    return raw;
}

List<SensorDataPerValue> SplitDataPoints(SensorData @event)
{
    return @event.value.Select(e =>
        new SensorDataPerValue
        {
            sensorId = @event.sensorId,
            type = e.type,
            unit = e.unit,
            value = e.value,
            timestamp = @event.timestamp
        }
    ).ToList();
}

// Aggregation (sum & count)
SensorDataPreAggregation AggregateEvents(
    SensorDataPerValue value,
    SensorDataPreAggregation aggregate) =>
    new()
    {
        sum = aggregate.sum + value.value,
        count = aggregate.count + 1,
        timestamp = value.timestamp,
        unit = value.unit
    };

SensorDataAggregation CalculateAverage(SensorDataPreAggregation value) =>
    new()
    {
        timestamp = value.timestamp,
        unit = value.unit,
        count = value.count,
        valueAvg = value.sum / value.count
    };

var builder = new StreamBuilder();

var stream = builder.Stream( // consume Kafka Topic
    "gko-csta-raw", new StringSerDes(), new SchemaAvroSerDes<SensorData>());

stream
    .MapValues((value, _) => ConvertTemperature(value), "ConvertToCelsius") // Fahrenheit -> Celsius
    .FlatMapValues((value, _) => SplitDataPoints(value), "SplitDataPoints") // One event per data point
    .GroupBy<SensorDataAggregationKey, SchemaAvroSerDes<SensorDataAggregationKey>,
        SchemaAvroSerDes<SensorDataPerValue>>( // Group by new key
        (_, v, _) => new SensorDataAggregationKey { sensorId = v.sensorId, type = v.type },
        "group") // -> repartition topic
    // Aggregate over tumling window
    .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMilliseconds(windowSizeInMillis)))
    .Aggregate(
        () => new SensorDataPreAggregation { count = 0, sum = 0.0d, unit = "", timestamp = "" }, // dummy initializer
        (_, value, aggregate) => AggregateEvents(value, aggregate), // calculate sum and count
        RocksDbWindows
            .As<SensorDataAggregationKey, SensorDataPreAggregation>("agg-store", null,
                TimeSpan.FromMilliseconds(windowSizeInMillis))
            .WithKeySerdes<SchemaAvroSerDes<SensorDataAggregationKey>>()
            .WithValueSerdes<SchemaAvroSerDes<SensorDataPreAggregation>>())
    .Suppress(
        SuppressedBuilder.UntilWindowClose<Windowed<SensorDataAggregationKey>, SensorDataPreAggregation>(
            TimeSpan.Zero, StrictBufferConfig.Unbounded()),
        "SuppressUpdate")
    .ToStream()
    .MapValues((value, _) => CalculateAverage(value), "CalculateAverage") // calculate average
    .To(
        "gko-csta-streamiz-aggregation", // Produce
        new TimeWindowedSerDes<SensorDataAggregationKey>(new SchemaAvroSerDes<SensorDataAggregationKey>(),
            windowSizeInMillis),
        new SchemaAvroSerDes<SensorDataAggregation>());


var topology = builder.Build();
var kstream = new KafkaStream(topology, streamConfig);

await kstream.StartAsync();