using System.Diagnostics;
using Arkhos.Core.Handlers;
using Arkhos.Core.Requests;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Requests.SchoolInfos;

namespace Arkhos.Api.Services;

public class CacheWarmupService(IServiceProvider serviceProvider) : BackgroundService
{
    private readonly TimeSpan _warmupInterval = TimeSpan.FromHours(4);
    private const int MACEIO_MUNICIPIO_ID = 2704302;
    private const int MACEIO_MICRORREGIAO_ID = 11;
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            int currentYear = 2025; 
            var sw = Stopwatch.StartNew();

            Console.WriteLine("==================================================");
            Console.WriteLine($"[CACHE] Iniciando ciclo de aquecimento robusto (Ano: {currentYear})");

            try
            {
                // Função auxiliar para garantir isolamento total de escopo e DbContext
                async Task RunInScope<THandler>(Func<THandler, Task> action) where THandler : notnull
                {
                    using var scope = serviceProvider.CreateScope();
                    var handler = scope.ServiceProvider.GetRequiredService<THandler>();
                    await action(handler);
                }

                await RunInScope<ISchoolInfosHandler>(h => h.GetByYearAsync(new GetSchoolInfoByYearRequest { Year = currentYear }));
                await RunInScope<ISchoolEnrollValuesHandler>(h => h.GetRegionEnrollmentSummaryByFilterAsync(new GetRegionEnrollmentSummaryByFilterRequest { Year = currentYear }));
                await RunInScope<ISchoolInfraValuesHandler>(h => h.GetRegionSummaryAsync(new GetRegionSummaryRequest { Year = currentYear }));
                await RunInScope<ISchoolRatingsHandler>(h => h.GetRegionRatingSummaryAsync(new GetRegionSummaryRequest { Year = currentYear }));

                
                // Nível: Município de Maceió
                await RunInScope<ISchoolEnrollValuesHandler>(h => h.GetRegionEnrollmentSummaryByFilterAsync(new GetRegionEnrollmentSummaryByFilterRequest { Year = currentYear, MunicipioId = MACEIO_MUNICIPIO_ID }));
                await RunInScope<ISchoolInfraValuesHandler>(h => h.GetRegionSummaryAsync(new GetRegionSummaryRequest { Year = currentYear, MunicipioId = MACEIO_MUNICIPIO_ID }));
                await RunInScope<ISchoolRatingsHandler>(h => h.GetRegionRatingSummaryAsync(new GetRegionSummaryRequest { Year = currentYear, MunicipioId = MACEIO_MUNICIPIO_ID }));

                // Nível: Microrregião de Maceió
                await RunInScope<ISchoolEnrollValuesHandler>(h => h.GetRegionEnrollmentSummaryByFilterAsync(new GetRegionEnrollmentSummaryByFilterRequest { Year = currentYear, MicrorregiaoId = MACEIO_MICRORREGIAO_ID }));
                await RunInScope<ISchoolInfraValuesHandler>(h => h.GetRegionSummaryAsync(new GetRegionSummaryRequest { Year = currentYear, MicrorregiaoId = MACEIO_MICRORREGIAO_ID }));
                await RunInScope<ISchoolRatingsHandler>(h => h.GetRegionRatingSummaryAsync(new GetRegionSummaryRequest { Year = currentYear, MicrorregiaoId = MACEIO_MICRORREGIAO_ID }));

                // 3. Aquecimento por Mesorregiões (Leste, Agreste, Sertão)
                for (int i = 1; i <= 3; i++)
                {
                    if (stoppingToken.IsCancellationRequested) break;

                    Console.WriteLine($"[CACHE] Processando Mesorregião {i}...");

                    await RunInScope<ISchoolEnrollValuesHandler>(h => h.GetRegionEnrollmentSummaryByFilterAsync(new GetRegionEnrollmentSummaryByFilterRequest { Year = currentYear, MesorregiaoId = i }));
                    await RunInScope<ISchoolInfraValuesHandler>(h => h.GetRegionSummaryAsync(new GetRegionSummaryRequest { Year = currentYear, MesorregiaoId = i }));
                    await RunInScope<ISchoolRatingsHandler>(h => h.GetRegionRatingSummaryAsync(new GetRegionSummaryRequest { Year = currentYear, MesorregiaoId = i }));

                    // Pequena pausa para não sufocar o pool de conexões do banco de dados
                    await Task.Delay(200, stoppingToken);
                }

                sw.Stop();
                Console.WriteLine($"[CACHE] Aquecimento finalizado com sucesso em {sw.ElapsedMilliseconds} ms!");
                Console.WriteLine($"[CACHE] O próximo ciclo rodará em {_warmupInterval.TotalHours} horas.");
                Console.WriteLine("==================================================");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CACHE ERROR] Falha no processo de aquecimento: {ex.Message}");
            }

            // Aguarda as 4 horas antes de recomeçar o loop.
            try
            {
                await Task.Delay(_warmupInterval, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }
        }
    }
}