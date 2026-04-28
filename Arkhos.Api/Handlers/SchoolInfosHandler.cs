using System.Diagnostics;
using Arkhos.Api.Data;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolInfo;
using Arkhos.Core.Requests.SchoolInfos;
using Arkhos.Core.Responses;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;

namespace Arkhos.Api.Handlers;

public class SchoolInfosHandler(AppDbContext context, IMemoryCache cache) : ISchoolInfosHandler
{
    // Semáforo estático para evitar "Cache Stampede"
    private static readonly SemaphoreSlim _semaphore = new(1, 1);

    public async Task<Response<ICollection<SchoolInfoMapDto>>> GetByYearAsync(GetSchoolInfoByYearRequest request, CancellationToken cancellationToken = default)
    {
        string cacheKey = $"SchoolMarkers_{request.Year}_{request.Dependencia}_{request.Limit}";

        // 1. Tenta pegar do cache primeiro (caminho rápido, sem travar)
        if (cache.TryGetValue(cacheKey, out ICollection<SchoolInfoMapDto>? cachedInfos))
        {
            return new Response<ICollection<SchoolInfoMapDto>>(cachedInfos!, message: "Cache hit!");
        }

        // 2. Aguarda a liberação do semáforo respeitando o cancelamento
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            // 3. O GetOrCreateAsync cuida de tentar pegar do cache de novo caso outra thread
            // já tenha feito o trabalho enquanto esta thread aguardava no semáforo.
            var schoolinfos = await cache.GetOrCreateAsync(cacheKey, async entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(6);

                var query = context.SchoolInfos
                    .AsNoTracking()
                    .Where(x => x.Ano == request.Year);

                if (request.Dependencia.HasValue)
                    query = query.Where(x => x.Dependencia == request.Dependencia.Value);

                var projection = query.Select(x => new SchoolInfoMapDto
                {
                    IdEscola = x.IdEscola,
                    NomeEscola = x.NomeEscola,
                    Endereco = x.Endereco ?? "Endereço não disponível",
                    Ano = x.Ano,
                    MunicipioId = x.CityInfoId,
                    NomeMunicipio = x.CityInfo.NomeMunicipio,
                    NomeMicrorregiao = x.CityInfo.NomeMicrorregiao, 
                    NomeMesorregiao = x.CityInfo.NomeMesorregiao,
                    Lat = x.Lat,
                    Lon = x.Lon,
                    Localizacao = x.Localizacao ?? 0,
                    MicrorregiaoId = x.CityInfo.IdMicrorregiao,
                    MesorregiaoId = x.CityInfo.IdMesorregiao,
                    Dependencia = x.Dependencia 
                });

                if (request.Limit.HasValue)
                    projection = projection.Take(request.Limit.Value);

                return await projection.ToListAsync(cancellationToken);
            });

            return new Response<ICollection<SchoolInfoMapDto>>(schoolinfos!, message: "Sucesso.");
        }
        catch (OperationCanceledException)
        {
            return new Response<ICollection<SchoolInfoMapDto>>(new List<SchoolInfoMapDto>(), 499, "Requisição cancelada.");
        }
        catch (Exception ex)
        {
            return new Response<ICollection<SchoolInfoMapDto>>(null, 500, $"Erro: {ex.Message}");
        }
        finally
        {
            _semaphore.Release();
        }
    }
}