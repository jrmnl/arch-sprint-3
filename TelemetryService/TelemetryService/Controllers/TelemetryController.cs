using Confluent.Kafka;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace TelemetryService.Controllers;

[ApiController]
[Route("api")]
public class TelemetryController : ControllerBase
{
    private readonly DeviceRepository _deviceRepository;
    private readonly TelemetryRepository _telemetryRepository;
    private readonly ProducerConfig _config;
    private readonly JsonSerializerOptions _serializerOptions;
    private readonly ILogger<TelemetryController> _logger;

    public TelemetryController(
        DeviceRepository deviceRepository,
        TelemetryRepository telemetryRepository,
        ProducerConfig config,
        ILogger<TelemetryController> logger)
    {
        _serializerOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
        _serializerOptions.Converters.Add(new JsonStringEnumConverter());
        _deviceRepository = deviceRepository ?? throw new ArgumentNullException(nameof(deviceRepository));
        _telemetryRepository = telemetryRepository ?? throw new ArgumentNullException(nameof(telemetryRepository));
        _config = config;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config.Acks = Acks.All;
        _config.MessageTimeoutMs = 3000;
    }

    [HttpPut("device/{deviceId}/telemetry")]
    public async Task<ActionResult<Guid>> AddTelemetry(
        [FromRoute] Guid deviceId,
        [FromBody] InputTelemetryData body,
        CancellationToken ct)
    {
        var device = await _deviceRepository
            .Find(deviceId, ct);

        if (device is null)
        {
            return this.UnprocessableEntity("Устройство не найдено");
        }

        var collectedAt = DateTimeOffset.UtcNow;

        await _telemetryRepository.Insert(
            new DbTelemetry
            {
                device_item_id = deviceId,
                collected_at = collectedAt,
                value = body.Value,
                value_type = body.ValueType,
            },
            ct);

        await SendToTopicAsync(
            new TelemetryData(
                deviceId,
                collectedAt,
                body.Value,
                body.ValueType),
            ct);

        return NoContent();
    }

    [HttpGet("device/{deviceId}/telemetry")]
    public async Task<ActionResult<List<OutputTelemetryData>>> GetTelemetry(
        [FromRoute] Guid deviceId,
        [FromQuery, Range(1, 100)] int topCount,
        CancellationToken ct)
    {
        var device = await _deviceRepository
            .Find(deviceId, ct);

        if (device is null)
        {
            return this.UnprocessableEntity("Устройство не найдено");
        }

        var result = await _telemetryRepository.GetLatest(deviceId, topCount, ct);
        return result
            .Select(x => new OutputTelemetryData
            {
                DeviceId = x.device_item_id,
                ValueType = x.value_type,
                Value = x.value,
                CollectedAt = x.collected_at,
            })
            .ToList();
    }


    private async Task SendToTopicAsync(
        TelemetryData data,
        CancellationToken ct)
    {
        using (var producer = new ProducerBuilder<string, string>(_config).Build())
        {
            var converter = new JsonStringEnumConverter();
            var serializedMessage = JsonSerializer.Serialize(data, _serializerOptions);
            await producer.ProduceAsync(
                "telemetry",
                new Message<string, string>
                {
                    Key = data.DeviceId.ToString(),
                    Value = serializedMessage,
                },
                ct);
            _logger.LogInformation(
                "Телеметрия отправлена в кафку для сообщения по устройству {deviceId}",
                data.DeviceId);

            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }
}


public record InputTelemetryData
{
    /// <summary>
    /// Значение телеметрии.
    /// </summary>
    [Required, StringLength(10)]
    public string Value { get; set; }

    /// <summary>
    /// Тип телеметрии, например, градусы цельсия, давление, влажность и проч.
    /// </summary>
    [Required, StringLength(50)]
    public string ValueType { get; set; }
}

public record OutputTelemetryData
{
    /// <summary>
    /// Идентификатор устройства.
    /// </summary>
    [Required]
    public Guid DeviceId { get; set; }

    /// <summary>
    /// Идентификатор устройства.
    /// </summary>
    [Required]
    public DateTimeOffset CollectedAt { get; set; }

    /// <summary>
    /// Значение телеметрии.
    /// </summary>
    [Required, StringLength(10)]
    public string Value { get; set; }

    /// <summary>
    /// Тип телеметрии, например, градусы цельсия, давление, влажность и проч.
    /// </summary>
    [Required, StringLength(50)]
    public string ValueType { get; set; }
}

public record TelemetryData(
    Guid DeviceId,
    DateTimeOffset CollectedAt,
    string Value,
    string ValueType);

public class TelemetryRepository
{
    private readonly NpgsqlConnection _conn;

    public TelemetryRepository(NpgsqlConnection conn)
    {
        _conn = conn ?? throw new ArgumentNullException(nameof(conn));
    }

    public async Task Insert(DbTelemetry device, CancellationToken ct)
    {
        const string sql = """
            INSERT INTO telemetry (
                 device_item_id
                ,collected_at
                ,value
                ,value_type
            ) 
            VALUES (
                 @device_item_id
                ,@collected_at
                ,@value
                ,@value_type
            )
            """;
        var cmd = new CommandDefinition(sql, device, cancellationToken: ct);
        await _conn.ExecuteAsync(cmd);
    }
    public async Task<IReadOnlyCollection<DbTelemetry>> GetLatest(
        Guid deviceId,
        int count,
        CancellationToken ct)
    {
        const string sql = """
            SELECT
                 device_item_id
                ,collected_at
                ,value
                ,value_type
            FROM telemetry
            WHERE device_item_id = @device_item_id
            ORDER BY collected_at DESC
            LIMIT (@count)
            """;
        var cmd = new CommandDefinition(sql, new { device_item_id = deviceId , count }, cancellationToken: ct);
        var list = await _conn.QueryAsync<DbTelemetry>(cmd);
        return list.ToList();
    }
}

public class DbTelemetry
{
    public Guid device_item_id { get; set; }
    public DateTimeOffset collected_at { get; set; }
    public string value { get; set; }
    public string value_type { get; set; }
}