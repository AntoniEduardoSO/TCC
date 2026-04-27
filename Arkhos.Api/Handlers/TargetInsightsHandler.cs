using System.Diagnostics;
using Arkhos.Api.Data;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models;
using Arkhos.Core.Requests.TargetInsights;
using Arkhos.Core.Responses;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;

namespace Arkhos.Api.Handlers;

public class TargetInsightsHandler(AppDbContext context, IMemoryCache cache) : ITargetInsightsHandler
{
    public async Task<Response<ICollection<TargetInsight>>> GetInsightsByFilterAsync(GetTargetInsightsByFilterRequest request)
    {
        string cacheKey = $"Insights_{request.Year}_{request.Level}_{request.Target}";

        try
        {
            var allInsights = await cache.GetOrCreateAsync(cacheKey, async entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(6); 

                var query = context.TargetInsights.AsNoTracking();

                if (request.Year is not null) query = query.Where(x => x.Ano == request.Year);
                if (request.Level is not null) query = query.Where(x => x.Level == request.Level);
                if (request.Target is not null) query = query.Where(x => x.IdAlvo == request.Target);

                return await query.ToListAsync();
            });

            if (allInsights == null || !allInsights.Any())
                return new Response<ICollection<TargetInsight>>(new List<TargetInsight>(), 200, "Sem insights.");

            var limit = request.Limit ?? 1;
            var random = new Random();
            var randomInsights = allInsights.OrderBy(x => random.Next()).Take(limit).ToList();

            return new Response<ICollection<TargetInsight>>(randomInsights, 200, "Retornado com sucesso o insights.");
        }
        catch
        {
            return new Response<ICollection<TargetInsight>>(null, 500, "Não foi possível consultar os Insight");
        }
    }
}