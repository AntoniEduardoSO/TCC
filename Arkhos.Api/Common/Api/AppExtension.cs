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
    private static void RunWebScraping(bool isProduction)
    {
        try
        {
            var commandPython = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "python" : "python3";
            var basePath = Directory.GetCurrentDirectory();

            var scriptPath = isProduction
            ? Path.Combine(basePath, "web-scraping", "main.py")
            : Path.GetFullPath(Path.Combine(basePath, "..", "web-scraping", "main.py"));

            Console.WriteLine($"[LOG] Procurando script Python em: {scriptPath}");

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

                var output = process.StandardOutput.ReadToEnd();
                var error = process.StandardError.ReadToEnd();

                if (process.ExitCode == 0)
                {
                    Console.WriteLine("Execução Python finalizada.");
                    Console.WriteLine("arkhos.db criado com sucesso!");
                    Console.WriteLine(output);
                }
                else
                {
                    Console.WriteLine("Erro na execução do script Python:");
                    Console.WriteLine(error);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Falha ao tentar executar o processo Python: {ex.Message}");
        }
    }
    public static void InitArkhosDatabase(this WebApplication app)
    {
        var isProduction = app.Environment.IsProduction();
        Console.WriteLine($"[LOG] Ambiente de Produção (Render/Docker): {isProduction}");

        var dbPath = isProduction ? "arkhos.db" : "../arkhos.db";
        var zipPath = isProduction ? "arkhos.zip" : "../arkhos.zip";
        var tempExtractPath = isProduction ? "temp_extract" : "../temp_extract";


        var downloadUrl = "https://www.dropbox.com/scl/fi/he9vq0un51f8m48kwfe7o/arkhos.zip?rlkey=6gw353bkxvckoqsej9dejp540&st=svr7z800&dl=1";

        Console.WriteLine($"[LOG] Procurando DB em: {dbPath}");

        if (!File.Exists(dbPath))
        {
            Console.WriteLine("[LOG] Banco não encontrado no disco.");
            if (!File.Exists(zipPath))
            {
                Console.WriteLine("[LOG] ZIP não encontrado. Iniciando Download do Dropbox...");
                using var client = new HttpClient();

                var response = client.GetAsync(downloadUrl).Result;
                response.EnsureSuccessStatusCode();

                using var fs = new FileStream(zipPath, FileMode.Create);
                response.Content.CopyToAsync(fs).Wait();

                Console.WriteLine("[LOG] Download concluído com sucesso!");

            }
            else
            {
                Console.WriteLine("[LOG] Arquivo ZIP já existe localmente. Pulando download.");
            }

            if (Directory.Exists(tempExtractPath)) Directory.Delete(tempExtractPath, true);
            Directory.CreateDirectory(tempExtractPath);
            ZipFile.ExtractToDirectory(zipPath, tempExtractPath, overwriteFiles: true);

            var extractedDbFile = Directory.GetFiles(tempExtractPath, "*.db", SearchOption.AllDirectories).FirstOrDefault();

            if (extractedDbFile != null)
            {
                Console.WriteLine($"[LOG] Arquivo DB encontrado no ZIP: {extractedDbFile}");
                File.Move(extractedDbFile, dbPath, overwrite: true);
                Console.WriteLine($"[LOG] Banco movido com sucesso para o alvo: {dbPath}");

                var tamanhoMb = new FileInfo(dbPath).Length / 1024.0 / 1024.0;
                Console.WriteLine($"[LOG] TAMANHO REAL DO BANCO EXTRAÍDO: {tamanhoMb:F2} MB");
            }
            else
            {
                Console.WriteLine("[LOG] ERRO CRÍTICO: Nenhum arquivo .db foi encontrado dentro do ZIP baixado!");
            }

            Directory.Delete(tempExtractPath, true);
            Console.WriteLine("[LOG] Extração concluída!");
        }
        else
        {
            Console.WriteLine("[LOG] Arquivo arkhos.db já existe. Pulando extração.");
        }

        using var scope = app.Services.CreateScope();

        var context = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        // if(!bancoRecemExtraido)
        //     context.Database.Migrate();

        Console.WriteLine($"[LOG] O EF ESTÁ CONECTANDO EM: {context.Database.GetDbConnection().ConnectionString}");


        if (!context.CityInfos.Any())
        {
            Console.WriteLine("[LOG] AVISO: Banco ainda está vazio após a extração!");

            if (isProduction)
            {
                Console.WriteLine("[LOG] Estamos no Render. O Python não será executado. Verifique o arquivo ZIP no Dropbox.");
            }
            else
            {
                Console.WriteLine("[LOG] Estamos no PC local. Iniciando script Python de fallback...");
                RunWebScraping(isProduction);
            }
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