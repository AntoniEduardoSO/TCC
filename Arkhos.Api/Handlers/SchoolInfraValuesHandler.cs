using System.Diagnostics;
using Arkhos.Api.Data;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolInfraValues;
using Arkhos.Core.Requests.SchoolInfraValues;
using Arkhos.Core.Responses;
using Microsoft.EntityFrameworkCore;

namespace Arkhos.Api.Handlers;



public class SchoolInfraValuesHandler(AppDbContext context) : ISchoolInfraValuesHandler
{
    public async Task<Response<ICollection<SchoolInfraValuesPedagogicalRoomsDto>>>
        GetPedagogicalRoomsByYearAsync(GetSchoolInfraValuesPedagogicalRoomsByYearRequest request)
    {
        var swTotal = Stopwatch.StartNew();
        var pedagogicalRoomAttributes = new[]
            {
                29, 36, 40, 41, 49, 50, 51, 52, 53, 56
            };

        try
        {
            var swDb = Stopwatch.StartNew();

            var query = context.SchoolInfraValues
                .AsNoTracking()
                .Where(x =>
                    x.Ano == request.Year &&
                    pedagogicalRoomAttributes.Contains(x.AtributoId))
                .GroupBy(x => new
                {
                    EscolaId = x.IdEscolaInfraValues,
                    x.Ano,
                    x.SchoolInfo.CityInfo.IdMesorregiao,
                    x.SchoolInfo.CityInfo.IdMicrorregiao,
                    x.SchoolInfo.CityInfo.MunicipioId
                })
                .Select(g => new SchoolInfraValuesPedagogicalRoomsDto
                {
                    EscolaId = g.Key.EscolaId,
                    Ano = g.Key.Ano,
                    MesorregiaoId = g.Key.IdMesorregiao,
                    MicrorregiaoId = g.Key.IdMicrorregiao,
                    MunicipioId = g.Key.MunicipioId,

                    Auditorio = g.Where(x => x.AtributoId == 29).Sum(x => x.Valor),
                    Biblioteca = g.Where(x => x.AtributoId == 36).Sum(x => x.Valor),
                    LaboratorioCiencias = g.Where(x => x.AtributoId == 40).Sum(x => x.Valor),
                    LaboratorioInformatica = g.Where(x => x.AtributoId == 41).Sum(x => x.Valor),
                    SalaAtelieArtes = g.Where(x => x.AtributoId == 49).Sum(x => x.Valor),
                    SalaMusicaCoral = g.Where(x => x.AtributoId == 50).Sum(x => x.Valor),
                    SalaEstudioDanca = g.Where(x => x.AtributoId == 51).Sum(x => x.Valor),
                    SalaMultiuso = g.Where(x => x.AtributoId == 52).Sum(x => x.Valor),
                    SalaEstudioGravacao = g.Where(x => x.AtributoId == 53).Sum(x => x.Valor),
                    SalaLeitura = g.Where(x => x.AtributoId == 56).Sum(x => x.Valor)
                });

            if (request.Limit.HasValue)
            {
                query = query.Take(request.Limit.Value);
            }

            var pedagogicalrooms = await query.ToListAsync();

            swDb.Stop();

            Console.WriteLine($"DB + Materialização: {swDb.ElapsedMilliseconds} ms");
            Console.WriteLine($"Quantidade: {pedagogicalrooms.Count}");

            var swSerialize = Stopwatch.StartNew();

            var json = System.Text.Json.JsonSerializer.Serialize(pedagogicalrooms);

            swSerialize.Stop();

            Console.WriteLine($"Serialização: {swSerialize.ElapsedMilliseconds} ms");
            Console.WriteLine($"Tamanho JSON: {System.Text.Encoding.UTF8.GetByteCount(json) / 1024.0 / 1024.0:F2} MB");

            swTotal.Stop();
            Console.WriteLine($"TOTAL (até aqui): {swTotal.ElapsedMilliseconds} ms");

            return new Response<ICollection<SchoolInfraValuesPedagogicalRoomsDto>>(pedagogicalrooms, 200, $"Salas pedagógicas carregadas com sucesso em {swTotal.ElapsedMilliseconds}ms.");
        }
        catch
        {
            return new Response<ICollection<SchoolInfraValuesPedagogicalRoomsDto>>(null, 500, "Não foi possível consultar as salas pedagógicas. [series]");
        }
    }

    public async Task<Response<ICollection<SchoolInfraValuesWellbeingDto>>> GetWellbeingByYearAsync(GetSchoolInfraValuesWellbeingByYearRequest request)
    {
        var swTotal = Stopwatch.StartNew();
        var wellbeingAttributes = new[]
            {
                27, 30, 31, 37, 38, 43, 44, 45, 46, 47, 48, 61, 101
            };

        try
        {
            var swDb = Stopwatch.StartNew();

            var query = context.SchoolInfraValues
                .AsNoTracking()
                .Where(x =>
                    x.Ano == request.Year &&
                    wellbeingAttributes.Contains(x.AtributoId))
                .GroupBy(x => new
                {
                    EscolaId = x.IdEscolaInfraValues,
                    x.Ano,
                    x.SchoolInfo.CityInfo.IdMesorregiao,
                    x.SchoolInfo.CityInfo.IdMicrorregiao,
                    x.SchoolInfo.CityInfo.MunicipioId
                })
                .Select(g => new SchoolInfraValuesWellbeingDto
                {
                    EscolaId = g.Key.EscolaId,
                    Ano = g.Key.Ano,
                    MesorregiaoId = g.Key.IdMesorregiao,
                    MicrorregiaoId = g.Key.IdMicrorregiao,
                    MunicipioId = g.Key.MunicipioId,

                    AlimentacaoEscolar = g.Where(x => x.AtributoId == 101).Sum(x => x.Valor),
                    Refeitorio = g.Where(x => x.AtributoId == 48).Sum(x => x.Valor),
                    Cozinha = g.Where(x => x.AtributoId == 37).Sum(x => x.Valor),
                    Despensa = g.Where(x => x.AtributoId == 38).Sum(x => x.Valor),
                    Terreirao = g.Where(x => x.AtributoId == 61).Sum(x => x.Valor),
                    PatioCoberto = g.Where(x => x.AtributoId == 43).Sum(x => x.Valor),
                    PatioDescoberto = g.Where(x => x.AtributoId == 44).Sum(x => x.Valor),
                    QuadraEsportes = g.Where(x => x.AtributoId == 47).Sum(x => x.Valor),
                    AreaVerde = g.Where(x => x.AtributoId == 27).Sum(x => x.Valor),
                    ParqueInfantil = g.Where(x => x.AtributoId == 45).Sum(x => x.Valor),
                    Piscina = g.Where(x => x.AtributoId == 46).Sum(x => x.Valor),
                    SalaRepousoAluno = g.Where(x => x.AtributoId == 58).Sum(x => x.Valor),
                    BanheiroAlunos = g.Where(x => x.AtributoId == 30).Sum(x => x.Valor),
                    BanheiroEducacaoInfantil = g.Where(x => x.AtributoId == 31).Sum(x => x.Valor), 
                });

            if (request.Limit.HasValue)
            {
                query = query.Take(request.Limit.Value);
            }

            var wellbeingDtos = await query.ToListAsync();

            swDb.Stop();

            Console.WriteLine($"DB + Materialização: {swDb.ElapsedMilliseconds} ms");
            Console.WriteLine($"Quantidade: {wellbeingDtos.Count}");

            var swSerialize = Stopwatch.StartNew();

            var json = System.Text.Json.JsonSerializer.Serialize(wellbeingDtos);

            swSerialize.Stop();

            Console.WriteLine($"Serialização: {swSerialize.ElapsedMilliseconds} ms");
            Console.WriteLine($"Tamanho JSON: {System.Text.Encoding.UTF8.GetByteCount(json) / 1024.0 / 1024.0:F2} MB");

            swTotal.Stop();
            Console.WriteLine($"TOTAL (até aqui): {swTotal.ElapsedMilliseconds} ms");

            return new Response<ICollection<SchoolInfraValuesWellbeingDto>>(wellbeingDtos, 200, $"Bem-estar escolar carregado com sucesso em {swTotal.ElapsedMilliseconds}ms.");
        }
        catch
        {
            return new Response<ICollection<SchoolInfraValuesWellbeingDto>>(null, 500, "Não foi possível consultar o bem-estar escolar. [series]");
        }
    }
}