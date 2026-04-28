using System.Diagnostics;
using Arkhos.Api.Data;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.CityInfo;
using Arkhos.Core.Requests.CityInfos;
using Arkhos.Core.Responses;
using Microsoft.EntityFrameworkCore;

namespace Arkhos.Api.Handlers;

public class CityInfosHandler(AppDbContext context) : ICityInfosHandler
{
    public async Task<Response<ICollection<CityInfoMapDto>>> GetByYearAsync(GetCityInfosByYearRequest request)
    {
        var swTotal = Stopwatch.StartNew();
        try
        {
            var swDb = Stopwatch.StartNew();
            
            var query = context.CityInfos
                .AsNoTracking()
                .Where(x => x.Ano == request.Year)
                .Select(x => new CityInfoMapDto
                {
                    Ano = x.Ano,
                    MunicipioId = x.MunicipioId,
                    NomeMunicipio = x.NomeMunicipio,
                    IdMesorregiao = x.IdMesorregiao,
                    NomeMesorregiao = x.NomeMesorregiao,
                    IdMicrorregiao = x.IdMicrorregiao,
                    NomeMicrorregiao = x.NomeMicrorregiao
                });
            
            if (request.Limit.HasValue)
            {
                query = query.Take(request.Limit.Value);
            }

            var cityinfos = await query.ToListAsync();

            swDb.Stop();

            Console.WriteLine($"DB + Materialização: {swDb.ElapsedMilliseconds} ms");
            Console.WriteLine($"Quantidade: {cityinfos.Count}");

            var swSerialize = Stopwatch.StartNew();
            var json = System.Text.Json.JsonSerializer.Serialize(cityinfos);
            swSerialize.Stop();

            Console.WriteLine($"Serialização: {swSerialize.ElapsedMilliseconds} ms");
            Console.WriteLine($"Tamanho JSON: {System.Text.Encoding.UTF8.GetByteCount(json) / 1024.0 / 1024.0:F2} MB");

            swTotal.Stop();
            Console.WriteLine($"TOTAL (até aqui): {swTotal.ElapsedMilliseconds} ms");

            return new Response<ICollection<CityInfoMapDto>>(cityinfos, 200, "Retornado com sucesso o cityinfos.");
        }
        catch (Exception ex)
        {
            return new Response<ICollection<CityInfoMapDto>>(null, 500, $"Não foi possível consultar as cidades. Erro: {ex.Message}");
        }
    }
}