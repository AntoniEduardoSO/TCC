using System.Diagnostics;
using Arkhos.Api.Data;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolInfo;
using Arkhos.Core.Requests.SchoolInfos;
using Arkhos.Core.Responses;
using Microsoft.EntityFrameworkCore;

namespace Arkhos.Api.Handlers;

public class SchoolInfosHandler(AppDbContext context) : ISchoolInfosHandler
{
    public async Task<Response<ICollection<SchoolInfoMapDto>>> GetByYearAsync(GetSchoolInfoByYearRequest request)
    {
        var swTotal = Stopwatch.StartNew();
        try
        {
            var swDb = Stopwatch.StartNew();
            
            var query = context.SchoolInfos
                .AsNoTracking()
                .Where(x => x.Ano == request.Year)
                .Select(x => new SchoolInfoMapDto
                {
                    IdEscola = x.IdEscola,
                    NomeEscola = x.NomeEscola,
                    Endereco= x.Endereco ?? string.Empty,
                    Ano = x.Ano,
                    MunicipioId = x.CityInfoId,
                    Lat = x.Lat,
                    Lon = x.Lon,
                    MicrorregiaoId = x.CityInfo.IdMicrorregiao,
                    MesorregiaoId = x.CityInfo.IdMesorregiao
                });

            if (request.Limit.HasValue)
            {
                query = query.Take(request.Limit.Value);
            }

            var schoolinfos = await query.ToListAsync();

            swDb.Stop();

            Console.WriteLine($"DB + Materialização: {swDb.ElapsedMilliseconds} ms");
            Console.WriteLine($"Quantidade: {schoolinfos.Count}");

            var swSerialize = Stopwatch.StartNew();

            var json = System.Text.Json.JsonSerializer.Serialize(schoolinfos);

            swSerialize.Stop();

            Console.WriteLine($"Serialização: {swSerialize.ElapsedMilliseconds} ms");
            Console.WriteLine($"Tamanho JSON: {System.Text.Encoding.UTF8.GetByteCount(json) / 1024.0 / 1024.0:F2} MB");

            swTotal.Stop();
            Console.WriteLine($"TOTAL (até aqui): {swTotal.ElapsedMilliseconds} ms");

            return new Response<ICollection<SchoolInfoMapDto>>(schoolinfos, message: "Retornado com sucesso o schoolinfos.");
        }
        catch
        {
            return new Response<ICollection<SchoolInfoMapDto>>(null, 500, "Não foi possível consultar as categorias");
        }
    }
}