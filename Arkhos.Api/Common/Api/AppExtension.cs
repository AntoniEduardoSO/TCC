namespace Arkhos.Api.Common.Api;

using System.Diagnostics;
using Arkhos.Api.Data;
using Microsoft.EntityFrameworkCore;

public static class AppExtension
{
    public static void InitWebScrapingEnviroment(this WebApplication app)
    {
        string baseDir = Directory.GetCurrentDirectory();
        string sqlFilePath = Path.Combine(baseDir, "..", "web-scraping", "database.sql");
        string pythonScriptPath = Path.Combine(baseDir, "..", "web-scraping", "main.py");
        
        if (File.Exists(sqlFilePath))
        {
            Console.WriteLine("To aqui.");
        }
        else
        {
            try
            {
                var processInfo = new ProcessStartInfo
                {
                    FileName = "python",
                    Arguments = $"\"{pythonScriptPath}\"",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true  
                };

                using var process = Process.Start(processInfo);

                if(process != null)
                {
                    string output = process.StandardOutput.ReadToEnd();
                    string errors = process.StandardError.ReadToEnd();

                    process.WaitForExit();

                    Console.WriteLine($"[Arkhos Init] Web Scraping finalizado. Saída do Python:\n{output}");

                    if (!string.IsNullOrEmpty(errors))
                    {
                        Console.WriteLine($"[Arkhos Init] Avisos/Erros do Python:\n{errors}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Arkhos Init - ERRO CRÍTICO] Falha ao executar o script Python: {ex.Message}");
            }
        }
    }
    public static void InitArkhosDatabase(this WebApplication app)
    {
        using var scope = app.Services.CreateScope();

        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        db.Database.Migrate();

        if (!db.CityInfos.Any())
        {
            Console.WriteLine("[Arkhos Init] Banco de dados limpo. Iniciando inserção de dados base...");

            string baseDir = Directory.GetCurrentDirectory();

            string sqlFilePath = Path.Combine(baseDir, "..", "web-scraping", "database.sql");

            if (File.Exists(sqlFilePath))
            {
                try
                {
                    string rawSql = File.ReadAllText(sqlFilePath);

                    using var command = db.Database.GetDbConnection().CreateCommand();
                    command.CommandText = rawSql;
                    command.CommandType = System.Data.CommandType.Text;

                    db.Database.OpenConnection();
                    command.ExecuteNonQuery();
                    db.Database.CloseConnection();
                    Console.WriteLine("[Arkhos Init] Carga de dados finalizada com sucesso!");
                }
                catch(Exception ex)
                {
                    Console.WriteLine($"[Arkhos Init - ERRO] Falha ao executar o script SQL: {ex.Message}");
                }
            }

        }
        else
        {
            Console.WriteLine("[Arkhos Init] O banco de dados já possui registros. Pulando a carga inicial.");
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