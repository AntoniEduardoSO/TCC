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
    public async Task<Response<ICollection<SchoolInfoMapDto>>> GetByYearAsync(GetSchoolInfoByYearRequest request)
    {
        string cacheKey = $"SchoolMarkers_{request.Year}_{request.Dependencia}_{request.Limit}";

        try
        {
            var schoolinfos = await cache.GetOrCreateAsync(cacheKey, async entry =>
            {
                // Deixa os marcadores em cache por 6 horas. Como é a requisição principal do mapa,
                // ela será resolvida via RAM instantaneamente!
                entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(6);

                var query = context.SchoolInfos
                    .AsNoTracking()
                    .Where(x => x.Ano == request.Year);

                if (request.Dependencia.HasValue)
                {
                    query = query.Where(x => x.Dependencia == request.Dependencia.Value);
                }

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
                {
                    projection = projection.Take(request.Limit.Value);
                }

                return await projection.ToListAsync();
            });

            return new Response<ICollection<SchoolInfoMapDto>>(schoolinfos, message: "Schoolinfos carregados com sucesso.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERRO FATAL NO BANCO]: {ex.Message}");
            return new Response<ICollection<SchoolInfoMapDto>>(null, 500, "Erro ao consultar informações das escolas.");
        }
    }
}