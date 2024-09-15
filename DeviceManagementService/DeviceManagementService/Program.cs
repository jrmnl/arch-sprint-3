using Confluent.Kafka;
using DbUp;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace DeviceManagementService;

public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        var config = builder.Configuration;
        var postgsConn = config.GetConnectionString("Postgres")
            ?? throw new Exception("Нет строки соединения!");

        var kafkaServers = config.GetValue<string>("Kafka:BootstrapServers")
            ?? throw new Exception("Нет BootstrapServers!");

        await RunMigrations(postgsConn);

        builder.Services.AddTransient(_ => new NpgsqlConnection(postgsConn));


        builder.Services.AddTransient(_ => new ProducerConfig
        {
            BootstrapServers = kafkaServers,
        });

        builder.Services
            .AddControllers()
            .AddJsonOptions(options =>
            {
                var converter = new JsonStringEnumConverter();
                options.JsonSerializerOptions.Converters.Add(converter);
            });

        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSwaggerGen();

        var app = builder.Build();
        app.UseSwagger();
        app.UseSwaggerUI();

        app.MapControllers();

        app.Run();
    }

    private static async Task RunMigrations(string connectionString)
    {
        await Do(
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

    public static async Task Do(Action action, TimeSpan retryInterval, int retryCount = 3)
    {
        var exceptions = new List<Exception>();

        for (int retry = 0; retry < retryCount; retry++)
        {
            try
            {
                action();
                return;
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
                await Task.Delay(retryInterval);
            }
        }
        exceptions.Add(new Exception(string.Format("Method call failed after {0} retries with {1} second intervals.", retryCount, retryInterval)));
        throw new AggregateException(exceptions);
    }
}
