namespace Arkhos.Api.Common.Api;

using System.Runtime.InteropServices;
using System.Diagnostics;
using Arkhos.Api.Data;
using Microsoft.EntityFrameworkCore;
using System.IO;
using System.IO.Compression;
using System.Net.Http;

using System.Text.Json;

public static class AppExtension
{

    private static bool IsScrapingDone(string scriptDirectory)
    {
        var flagPath = Path.Combine(scriptDirectory, "status", "scraping_done.flag");

        if (!File.Exists(flagPath))
            return false;

        try
        {
            var json = File.ReadAllText(flagPath);
            var doc = JsonDocument.Parse(json);
            var status = doc.RootElement.GetProperty("status").GetString();
            return status == "done";
        }
        catch
        {
            return false;
        }
    }
    private static void RunWebScraping(string scriptPath, string scriptDirectory)
    {
        try
        {
            var commandPython = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "python" : "python3";
            var modo = IsScrapingDone(scriptDirectory) ? "2" : "1";

            var startInfo = new ProcessStartInfo
            {
                FileName = commandPython,
                Arguments = $"\"{scriptPath}\" {modo}",
                WorkingDirectory = scriptDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using var process = Process.Start(startInfo);
            if (process != null)
            {
                process.WaitForExit();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
    }
    public static void InitArkhosDatabase(this WebApplication app)
    {
        var isProduction = app.Environment.IsProduction();

        using var scope = app.Services.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        try
        {
            if (isProduction)
            {
                Console.WriteLine("[DEBUG] Tentando acessar os dados do Turso...");
                
                var testeConexao = context.CityInfos.Take(1).ToList();
                Console.WriteLine($"[SUCESSO] Conectou no Turso! Cidades lidas: {testeConexao.Count}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERRO FATAL NO EF CORE]: {ex.Message}");
            Console.WriteLine($"[STACK TRACE DETALHADO]: {ex.StackTrace}");
        }

        if (isProduction)
        {
            return;
        }


        var dbPath = "../arkhos.db";
        var scriptPath = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "..", "web-scraping", "main.py"));
        var scriptDirectory = Path.GetDirectoryName(scriptPath) ?? string.Empty;

        if (!File.Exists(dbPath) || !context.CityInfos.Any())
        {
            RunWebScraping(scriptPath, scriptDirectory);
        }
    }
    public static void ConfigureDevEnvironment(this WebApplication app)
    {
        app.UseSwagger();
        app.UseSwaggerUI();
        // app.MapSwagger().RequireAuthorization();
    }

    public static void UseSecurity(this WebApplication app)
    {
        app.UseAuthentication();
        app.UseAuthorization();
    }
}