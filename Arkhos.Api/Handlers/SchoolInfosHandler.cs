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
                Lat = x.Lat,
                Lon = x.Lon,
                MicrorregiaoId = x.CityInfo.IdMicrorregiao,
                MesorregiaoId = x.CityInfo.IdMesorregiao,
                Dependencia = x.Dependencia 
            });

            if (request.Limit.HasValue)
            {
                projection = projection.Take(request.Limit.Value);
            }

            var schoolinfos = await projection.ToListAsync();

            swTotal.Stop();
            return new Response<ICollection<SchoolInfoMapDto>>(schoolinfos, message: "Schoolinfos carregados com sucesso.");
        }
        catch (Exception ex)
        {
            return new Response<ICollection<SchoolInfoMapDto>>(null, 500, "Erro ao consultar informações das escolas.");
        }
    }
}