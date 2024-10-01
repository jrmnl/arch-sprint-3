using Confluent.Kafka;
using Dapper;
using Microsoft.AspNetCore.Mvc.Formatters;
using Npgsql;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace TelemetryService;

public interface IWorker
{
    Task DoWork(CancellationToken stoppingToken);
}

public class ScopedService<T> : BackgroundService where T : IWorker
{
    private readonly IServiceProvider _serviceProvider;

    public ScopedService(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider
            ?? throw new ArgumentNullException(nameof(serviceProvider));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Run(async () =>
        {
            using var scope = _serviceProvider.CreateScope();
            var service = scope.ServiceProvider.GetService<T>()
                ?? throw new Exception($"Can't resolve service {nameof(T)}");
            await Retrier.DoAsync(
                service.DoWork(stoppingToken),
                TimeSpan.FromSeconds(3),
                5);
        });
    }
}


public class DeviceConsumerWorker : IWorker
{
    private readonly JsonSerializerOptions _serializerOptions;
    private readonly ILogger<DeviceConsumerWorker> _logger;
    private readonly ConsumerConfig _consumerConfig;
    private readonly DeviceRepository _deviceRepository;

    public DeviceConsumerWorker(
        DeviceRepository deviceRepository,
        ConsumerConfig config,
        ILogger<DeviceConsumerWorker> logger)
    {
        _deviceRepository = deviceRepository ?? throw new ArgumentNullException(nameof(deviceRepository));
        _consumerConfig = config ?? throw new ArgumentNullException(nameof(config));
        _consumerConfig.GroupId = "telemetry_service";
        _consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;

        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _serializerOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
        _serializerOptions.Converters.Add(new JsonStringEnumConverter());
    }

    public async Task DoWork(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using (var consumer = new ConsumerBuilder<string, string>(_consumerConfig).Build())
                {
                    consumer.Subscribe("device");

                    _logger.LogInformation("Подписан на топик, слушаю...");

                    while (!stoppingToken.IsCancellationRequested)
                    {
                        var result = consumer.Consume(stoppingToken);
                        var body = JsonSerializer.Deserialize<DeviceEventMessage<NewDevice>>(
                            result.Message.Value,
                            _serializerOptions)
                            ?? throw new Exception("Неудалось десериализовать сообщение");
                        var deviceId = Guid.Parse(result.Message.Key);

                        if (body.DeviceEvent == DeviceEvent.Registered)
                        {
                            await _deviceRepository.Insert(
                                new DbDeviceItem
                                {
                                    device_item_id = deviceId,
                                    device_type = body.Details.DeviceType,
                                    name = body.Details.Name,
                                    model = body.Details.Model,
                                    device_address = body.Details.DeviceAddress,
                                    serial_number = body.Details.SerialNumber,
                                    status = body.Details.Status,
                                    user_id = body.Details.UserId,
                                    home_id = body.Details.HomeId,
                                },
                                stoppingToken);
                            _logger.LogInformation(
                                "Добавлено устройство {deviceId}",
                                deviceId);
                        }
                        else if (body.DeviceEvent == DeviceEvent.Deleted)
                        {
                            await _deviceRepository.Delete(deviceId, stoppingToken);
                            _logger.LogInformation(
                                "Удалено устройство {deviceId}",
                                deviceId);
                        }
                        else
                        {
                            _logger.LogWarning(
                                "Неизвестный тип ивента {eventType} для устройства{deviceId}",
                                body.DeviceEvent,
                                deviceId);
                        }
                    }

                    consumer.Close();
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Отмена обработки событий");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка во время обработки событий");
                await Task.Delay(5000);
            }
        }
    }
}

public class DeviceRepository
{
    private readonly NpgsqlConnection _conn;

    public DeviceRepository(NpgsqlConnection conn)
    {
        _conn = conn ?? throw new ArgumentNullException(nameof(conn));
    }

    public async Task Insert(DbDeviceItem device, CancellationToken ct)
    {
        const string sql = """
            INSERT INTO device_item (
                 device_item_id
                ,device_type
                ,name
                ,model
                ,device_address
                ,serial_number
                ,status
                ,user_id
                ,home_id
            ) 
            VALUES (
                 @device_item_id
                ,@device_type
                ,@name
                ,@model
                ,@device_address
                ,@serial_number
                ,@status
                ,@user_id
                ,@home_id
            )
            ON CONFLICT DO NOTHING
            """;
        var cmd = new CommandDefinition(sql, device, cancellationToken: ct);
        await _conn.ExecuteAsync(cmd);
    }

    public async Task Delete(Guid deviceId, CancellationToken ct)
    {
        const string sql = """
            DELETE FROM device_item 
            WHERE device_item_id = @device_id
            """;
        var cmd = new CommandDefinition(sql, new { device_id = deviceId} , cancellationToken: ct);
        await _conn.ExecuteAsync(cmd);
    }

    public async Task<DbDeviceItem?> Find(Guid deviceId, CancellationToken ct)
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
        var result = await _conn.QuerySingleOrDefaultAsync<DbDeviceItem>(cmd);
        return result;
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
public class NewDevice
{
    public string DeviceType { get; set; }

    public string Name { get; set; }

    public string Model { get; set; }

    public string DeviceAddress { get; set; }

    public string SerialNumber { get; set; }

    public string Status { get; set; }

    public Guid UserId { get; set; }

    public Guid HomeId { get; set; }
}