using System.Diagnostics;
using Arkhos.Api.Data;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models;
using Arkhos.Core.Requests.TargetInsights;
using Arkhos.Core.Responses;
using Microsoft.EntityFrameworkCore;

namespace Arkhos.Api.Handlers;

public class TargetInsightsHandler(AppDbContext context) : ITargetInsightsHandler
{
    public async Task<Response<ICollection<TargetInsight>>> GetInsightsByFilterAsync(GetTargetInsightsByFilterRequest request)
    {
        var swTotal = Stopwatch.StartNew();
        try
        {
            var swDb = Stopwatch.StartNew();

            var query = context.TargetInsights
                .AsNoTracking();

            if(request.Year is not null)
            {
                query = query
                    .Where(x => x.Ano == request.Year);
            }

            if(request.Level is not null)
            {
                query = query
                    .Where(x => x.Level == request.Level);
            }

            if(request.Target is not null)
            {
                query = query
                    .Where(x => x.IdAlvo == request.Target);
            }

            query = query.OrderBy(x => EF.Functions.Random());

            if (request.Limit.HasValue)
            {
                query = query.Take(request.Limit.Value);
            }


            var insights = await query.ToListAsync();

            swDb.Stop();


            Console.WriteLine($"DB + Materialização: {swDb.ElapsedMilliseconds} ms");
            Console.WriteLine($"Quantidade: {insights.Count}");

            var swSerialize = Stopwatch.StartNew();

            var json = System.Text.Json.JsonSerializer.Serialize(insights);

            swSerialize.Stop();

            Console.WriteLine($"Serialização: {swSerialize.ElapsedMilliseconds} ms");
            Console.WriteLine($"Tamanho JSON: {System.Text.Encoding.UTF8.GetByteCount(json) / 1024.0 / 1024.0:F2} MB");

            swTotal.Stop();
            Console.WriteLine($"TOTAL (até aqui): {swTotal.ElapsedMilliseconds} ms");

            return new Response<ICollection<TargetInsight>>(insights, message: "Retornado com sucesso o insights.");
        }
        catch
        {
            return new Response<ICollection<TargetInsight>>(null, 500, "Não foi possível consultar os Insight");
        }
    }
}