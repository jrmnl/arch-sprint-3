
using Confluent.Kafka;
using DbUp;
using Npgsql;
using System.Reflection;
using System.Text;
using TelemetryService.Controllers;

namespace TelemetryService;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;

        var builder = WebApplication.CreateBuilder(args);

        var config = builder.Configuration;
        var postgsConn = config.GetConnectionString("Postgres")
            ?? throw new Exception("Нет строки соединения!");

        var kafkaServers = config.GetValue<string>("Kafka:BootstrapServers")
            ?? throw new Exception("Нет BootstrapServers!");

        await RunMigrations(postgsConn);

        builder.Services.AddTransient(_ => new NpgsqlConnection(postgsConn));
        builder.Services.AddTransient<DeviceRepository>();
        builder.Services.AddTransient<TelemetryRepository>();
        

        builder.Services.AddSingleton<DeviceConsumerWorker>();
        builder.Services.AddHostedService<ScopedService<DeviceConsumerWorker>>();

        builder.Services.AddSingleton(_ => new ConsumerConfig
        {
            BootstrapServers = kafkaServers,
        });
        builder.Services.AddSingleton(_ => new ProducerConfig
        {
            BootstrapServers = kafkaServers,
        });

        builder.Services.AddControllers();
        // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSwaggerGen();

        var app = builder.Build();

        // Configure the HTTP request pipeline.
        app.UseSwagger();
        app.UseSwaggerUI();

        app.MapControllers();

        app.Run();
    }

    private static async Task RunMigrations(string connectionString)
    {
        await Retrier.Do(
            () => EnsureDatabase.For.PostgresqlDatabase(connectionString),
            TimeSpan.FromSeconds(3),
            5);

        var upgrader =
            DeployChanges.To
                .PostgresqlDatabase(connectionString)
                .WithScriptsEmbeddedInAssembly(Assembly.GetExecutingAssembly())
                .LogToConsole()
                .Build();

        var result = upgrader.PerformUpgrade();

        if (!result.Successful)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(result.Error);
            Console.ResetColor();
        }

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("Миграция удалась!");
        Console.ResetColor();
    }

}
