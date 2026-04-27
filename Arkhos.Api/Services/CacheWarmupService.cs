using Arkhos.Core.Handlers;
using Arkhos.Core.Requests;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Requests.SchoolInfos;

namespace Arkhos.Api.Services;

public class CacheWarmupService(IServiceProvider serviceProvider) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

        using var scope = serviceProvider.CreateScope();
        
        var enrollHandler = scope.ServiceProvider.GetRequiredService<ISchoolEnrollValuesHandler>();
        var infraHandler = scope.ServiceProvider.GetRequiredService<ISchoolInfraValuesHandler>();
        var ratingHandler = scope.ServiceProvider.GetRequiredService<ISchoolRatingsHandler>();
        var infoHandler = scope.ServiceProvider.GetRequiredService<ISchoolInfosHandler>();

        int currentYear = 2024;

        try
        {
            Console.WriteLine("Iniciando aquecimento do Cache");

            await infoHandler.GetByYearAsync(new GetSchoolInfoByYearRequest { Year = currentYear, Dependencia = null });

            await enrollHandler.GetRegionEnrollmentSummaryByFilterAsync(new GetRegionEnrollmentSummaryByFilterRequest { Year = currentYear });
            await infraHandler.GetRegionSummaryAsync(new GetRegionSummaryRequest { Year = currentYear });
            await ratingHandler.GetRegionRatingSummaryAsync(new GetRegionSummaryRequest { Year = currentYear });


            for (int i = 1; i <= 3; i++)
            {
                await enrollHandler.GetRegionEnrollmentSummaryByFilterAsync(new GetRegionEnrollmentSummaryByFilterRequest { Year = currentYear, MesorregiaoId = i });
                await infraHandler.GetRegionSummaryAsync(new GetRegionSummaryRequest { Year = currentYear, MesorregiaoId = i });
                await ratingHandler.GetRegionRatingSummaryAsync(new GetRegionSummaryRequest { Year = currentYear, MesorregiaoId = i });
            }

            Console.WriteLine("Aquecimento do Cache finalizado.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro no aquecimento do cache: {ex.Message}");
        }
    }
}