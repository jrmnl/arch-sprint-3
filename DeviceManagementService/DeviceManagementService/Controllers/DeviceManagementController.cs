using Confluent.Kafka;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Npgsql;
using System;
using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace DeviceManagementService.Controllers;

[ApiController]
[Route("api/devices")]
public class DeviceManagementController : ControllerBase
{
    private readonly ILogger<DeviceManagementController> _logger;
    private readonly NpgsqlConnection _conn;
    private readonly ProducerConfig _config;
    private readonly JsonSerializerOptions _serializerOptions;

    public DeviceManagementController(
        NpgsqlConnection conn,
        ProducerConfig config,
        ILogger<DeviceManagementController> logger)
    {
        _conn = conn;
        _serializerOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
        _serializerOptions.Converters.Add(new JsonStringEnumConverter());

        _config = config;
        _config.Acks = Acks.All;
        _config.MessageTimeoutMs = 3000;
        _logger = logger;
    }

    [HttpPost]
    public async Task<Guid> Register([FromBody] NewDevice body, CancellationToken ct)
    {
        const string sql = """
            INSERT INTO device_item (
                 device_type
                ,name
                ,model
                ,device_address
                ,serial_number
                ,status
                ,user_id
                ,home_id
            ) 
            VALUES (
                 @device_type
                ,@name
                ,@model
                ,@device_address
                ,@serial_number
                ,@status
                ,@user_id
                ,@home_id
            )
            RETURNING device_item_id;
            """;
        var cmd = new CommandDefinition(sql, body.ToDbDeviceItem(), cancellationToken: ct);
        var deviceId = await _conn.ExecuteScalarAsync<Guid>(cmd);

        await SendToTopicAsync(
            deviceId,
            DeviceEvent.Registered,
            body,
            ct);

        return deviceId;
    }

    [HttpGet("{deviceId}")]
    public async Task<DeviceItem> Get([FromRoute] Guid deviceId, CancellationToken ct)
    {
        const string sql = """
            SELECT 
                 device_item_id
                ,device_type
                ,name
                ,model
                ,device_address
                ,serial_number
                ,status
                ,user_id
                ,home_id
            FROM device_item
            WHERE device_item_id = @device_id;
            """;
        var cmd = new CommandDefinition(sql, new { device_id = deviceId }, cancellationToken: ct);
        var result = await _conn.QuerySingleAsync<DbDeviceItem>(cmd);
        return result.ToDeviceItem();
    }

    [HttpDelete("{deviceId}")]
    public async Task Delete([FromRoute] Guid deviceId, CancellationToken ct)
    {
        const string sql = """
            DELETE FROM device_item
            WHERE device_item_id = @device_id;
            """;
        var cmd = new CommandDefinition(sql, new { device_id = deviceId }, cancellationToken: ct);
        await _conn.ExecuteAsync(cmd);

        await SendToTopicAsync(deviceId, DeviceEvent.Deleted, ct);
    }

    private Task SendToTopicAsync(
        Guid id,
        DeviceEvent eventType,
        CancellationToken ct)
    {
        return SendToTopicAsync<string>(id, eventType, null, ct);
    }

    private async Task SendToTopicAsync<T>(
        Guid id,
        DeviceEvent eventType,
        T? details,
        CancellationToken ct)
    {
        using (var producer = new ProducerBuilder<string, string>(_config).Build())
        {
            var converter = new JsonStringEnumConverter();
            var message = new DeviceEventMessage<T>(id, eventType, details);
            var partition = Math.Abs(id.GetHashCode()) % 3;
            var serializedMessage = JsonSerializer.Serialize(message, _serializerOptions);
            Console.WriteLine(_config.BootstrapServers);
            await producer.ProduceAsync(
                new TopicPartition("device", new Partition(partition)),
                new Message<string, string>
                {
                    Key = id.ToString(),
                    Value = serializedMessage,
                },
                ct);

            _logger.LogInformation("Сообщение {eventType} отправлено для устройства {deviceId}", eventType, id);


            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }
}

record DeviceEventMessage<T>(
    Guid DeviceId,
    DeviceEvent DeviceEvent,
    T? Details);

enum DeviceEvent
{
    Registered,
    Deleted,
}

internal static class DbDeviceItemExtensions
{
    public static DeviceItem ToDeviceItem(this DbDeviceItem item)
    {
        return new DeviceItem
        {
            Id = item.device_item_id,
            DeviceType = item.device_type,
            Name = item.name,
            Model = item.model,
            DeviceAddress = item.device_address,
            SerialNumber = item.serial_number,
            Status = item.status,
            UserId = item.user_id,
            HomeId = item.home_id,
        };
    }

    public static DbDeviceItem ToDbDeviceItem(this NewDevice item)
    {
        return new DbDeviceItem
        {
            device_type = item.DeviceType,
            name = item.Name,
            model = item.Model,
            device_address = item.DeviceAddress,
            serial_number = item.SerialNumber,
            status = item.Status.ToString(),
            user_id = item.UserId,
            home_id = item.HomeId,
        };
    }
}


public class NewDevice
{
    [Required, StringLength(255)]
    public string DeviceType { get; set; }

    [Required, StringLength(255)]
    public string Name { get; set; }

    [Required, StringLength(255)]
    public string Model { get; set; }

    [Required, StringLength(255)]
    public string DeviceAddress { get; set; }

    [Required, StringLength(255)]
    public string SerialNumber { get; set; }

    [Required, StringLength(50)]
    public string Status { get; set; }

    [Required]
    public Guid UserId { get; set; }

    [Required]
    public Guid HomeId { get; set; }
}

public class DeviceItem
{
    [Required]
    public Guid Id { get; set; }

    [Required, StringLength(255)]
    public string DeviceType { get; set; }

    [Required, StringLength(255)]
    public string Name { get; set; }

    [Required, StringLength(255)]
    public string Model { get; set; }

    [Required, StringLength(255)]
    public string DeviceAddress { get; set; }

    [Required, StringLength(255)]
    public string SerialNumber { get; set; }

    [Required, StringLength(50)]
    public string Status { get; set; }

    [Required]
    public Guid UserId { get; set; }

    [Required]
    public Guid HomeId { get; set; }
}
public class DbDeviceItem
{
    public Guid device_item_id { get; set; }

    public string device_type { get; set; }

    public string name { get; set; }

    public string model { get; set; }

    public string device_address { get; set; }

    public string serial_number { get; set; }

    public string status { get; set; }

    public Guid user_id { get; set; }

    public Guid home_id { get; set; }
}
