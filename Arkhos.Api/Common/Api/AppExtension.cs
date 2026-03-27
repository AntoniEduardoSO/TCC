namespace Arkhos.Api.Common.Api;

using System.Runtime.InteropServices;
using System.Diagnostics;
using Arkhos.Api.Data;
using Microsoft.EntityFrameworkCore;

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
    private static void RunWebScraping()
    {
        try
        {
            var commandPython = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "python" : "python3";
            var basePath = Directory.GetCurrentDirectory();

            var scriptPath = Path.GetFullPath(Path.Combine(basePath,"..", "web-scraping", "main.py"));

            if (!File.Exists(scriptPath))
            {
                Console.WriteLine($"Erro: Script não encontrado no caminho {scriptPath}");
                return;
            }

            var scriptDirectory = Path.GetDirectoryName(scriptPath) ?? string.Empty;

            var modo = IsScrapingDone(scriptDirectory) ? "2" : "1";

            var startInfo = new ProcessStartInfo
            {
                FileName = commandPython,
                Arguments = $"\"{scriptPath}\" {modo}",
                WorkingDirectory = Path.GetDirectoryName(scriptPath),
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using var process = Process.Start(startInfo);
            if (process != null)
            {
                process.WaitForExit(); // Aguarda o Python terminar de salvar no banco

                var output = process.StandardOutput.ReadToEnd();
                var error = process.StandardError.ReadToEnd();

                if (process.ExitCode == 0)
                {
                    Console.WriteLine("Execução Python finalizada.");
                    Console.WriteLine(output);
                }
                else
                {
                    Console.WriteLine("Erro na execução do script Python:");
                    Console.WriteLine(error);
                }
            }
        }
        catch(Exception ex)
        {
            Console.WriteLine($"Falha ao tentar executar o processo Python: {ex.Message}");
        }
    }

    public static void InitArkhosDatabase(this WebApplication app)
    {
        using var scope = app.Services.CreateScope();

        var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        context.Database.Migrate();

        if(!context.CityInfos.Any())
        {
            Console.WriteLine("Banco vazio.");
            RunWebScraping();
        }
        else
        {
            Console.WriteLine("Banco de dados ja populado.");
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