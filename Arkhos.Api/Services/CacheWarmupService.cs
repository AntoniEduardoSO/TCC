using System.Diagnostics;
using Arkhos.Core.Handlers;
using Arkhos.Core.Requests;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Requests.SchoolInfos;

namespace Arkhos.Api.Services;

public class CacheWarmupService(IServiceProvider serviceProvider) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Aguarda a estabilização da API antes de iniciar
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

        int currentYear = 2024;
        var sw = Stopwatch.StartNew();

        Console.WriteLine("==================================================");
        Console.WriteLine("[CACHE] Iniciando aquecimento robusto do Cache");

        try
        {
            // Função auxiliar para garantir isolamento total de escopo e DbContext
            async Task RunInScope<THandler>(Func<THandler, Task> action) where THandler : notnull
            {
                using var scope = serviceProvider.CreateScope();
                var handler = scope.ServiceProvider.GetRequiredService<THandler>();
                await action(handler);
            }

            // 1. Aquecimento de Dados Globais (Nível Estadual)
            await RunInScope<ISchoolInfosHandler>(h => 
                h.GetByYearAsync(new GetSchoolInfoByYearRequest { Year = currentYear }));

            await RunInScope<ISchoolEnrollValuesHandler>(h => 
                h.GetRegionEnrollmentSummaryByFilterAsync(new GetRegionEnrollmentSummaryByFilterRequest { Year = currentYear }));

            await RunInScope<ISchoolInfraValuesHandler>(h => 
                h.GetRegionSummaryAsync(new GetRegionSummaryRequest { Year = currentYear }));

            await RunInScope<ISchoolRatingsHandler>(h => 
                h.GetRegionRatingSummaryAsync(new GetRegionSummaryRequest { Year = currentYear }));

            // 2. Aquecimento por Mesorregiões (Ex: 1, 2 e 3)
            for (int i = 1; i <= 3; i++)
            {
                if (stoppingToken.IsCancellationRequested) break;

                Console.WriteLine($"[CACHE] Processando Mesorregião {i}...");

                await RunInScope<ISchoolEnrollValuesHandler>(h => 
                    h.GetRegionEnrollmentSummaryByFilterAsync(new GetRegionEnrollmentSummaryByFilterRequest 
                    { 
                        Year = currentYear, 
                        MesorregiaoId = i 
                    }));

                await RunInScope<ISchoolInfraValuesHandler>(h => 
                    h.GetRegionSummaryAsync(new GetRegionSummaryRequest 
                    { 
                        Year = currentYear, 
                        MesorregiaoId = i 
                    }));

                await RunInScope<ISchoolRatingsHandler>(h => 
                    h.GetRegionRatingSummaryAsync(new GetRegionSummaryRequest 
                    { 
                        Year = currentYear, 
                        MesorregiaoId = i 
                    }));

                await Task.Delay(200, stoppingToken);
            }

            sw.Stop();
            Console.WriteLine($"[CACHE] Aquecimento finalizado com sucesso em {sw.ElapsedMilliseconds} ms!");
            Console.WriteLine("==================================================");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CACHE ERROR] Falha no processo de aquecimento: {ex.Message}");
        }
    }
}