using System.Diagnostics;
using Arkhos.Api.Data;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolEnrollValues;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;
using Microsoft.EntityFrameworkCore;

namespace Arkhos.Api.Handlers;

public class SchoolEnrollValuesHandler(AppDbContext context) : ISchoolEnrollValuesHandler
{
    public async Task<Response<ICollection<SchoolEnrollValuesGovernanceDto>>> GetGovernanceByYearAsync(GetSchoolEnrollValuesGovernanceByYearRequest request)
    {
        var swTotal = Stopwatch.StartNew();
        double[] governance_ids = [8,6,15,16,19,20,21];
        try
        {
            var swDb = Stopwatch.StartNew();

            var query = context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == request.Year
                    && governance_ids.Contains(x.AtributoId))
                .GroupBy(x => new
                {
                    x.IdEscolaEnrollValues,
                    x.Ano,
                    x.SchoolInfo.CityInfo.IdMesorregiao,
                    x.SchoolInfo.CityInfo.IdMicrorregiao,
                    x.SchoolInfo.CityInfo.MunicipioId
                })
                .Select(g => new SchoolEnrollValuesGovernanceDto
                {
                    EscolaId = g.Key.IdEscolaEnrollValues,
                    Ano = g.Key.Ano,
                    MesorregiaoId = g.Key.IdMesorregiao,
                    MicrorregiaoId = g.Key.IdMicrorregiao,
                    MunicipioId = g.Key.MunicipioId,

                    Psicologo = g.Sum(x => x.AtributoId == 8 ? x.Valor : 0),
                    Fonaudiologo = g.Sum(x => x.AtributoId == 6 ? x.Valor : 0),
                    AssistenteSocial = g.Sum(x => x.AtributoId == 15 ? x.Valor : 0),
                    TradutorLibras = g.Sum(x => x.AtributoId == 16 ? x.Valor : 0),
                    AssociacaoPaiMestres = g.Sum(x => x.AtributoId == 19 ? x.Valor : 0),
                    ConselhoEscolar = g.Sum(x => x.AtributoId == 20 ? x.Valor : 0),
                    GremioEstudantil = g.Sum(x => x.AtributoId == 21 ? x.Valor : 0),
                });
        

            if (request.Limit.HasValue)
            {
                query = query
                    .OrderBy(x => x.EscolaId)
                    .Take(request.Limit.Value);
            }

            var governanceDtos = await query.ToListAsync();

            swDb.Stop();

            Console.WriteLine($"DB + Materialização: {swDb.ElapsedMilliseconds} ms");
            Console.WriteLine($"Quantidade: {governanceDtos.Count}");

            var swSerialize = Stopwatch.StartNew();

            var json = System.Text.Json.JsonSerializer.Serialize(governanceDtos);

            swSerialize.Stop();

            Console.WriteLine($"Serialização: {swSerialize.ElapsedMilliseconds} ms");
            Console.WriteLine($"Tamanho JSON: {System.Text.Encoding.UTF8.GetByteCount(json) / 1024.0 / 1024.0:F2} MB");

            swTotal.Stop();
            Console.WriteLine($"TOTAL (até aqui): {swTotal.ElapsedMilliseconds} ms");

            return new Response<ICollection<SchoolEnrollValuesGovernanceDto>>(governanceDtos, 200, $"Matrículas carregadas com sucesso em {swTotal.ElapsedMilliseconds}ms.");
        }
        catch
        {
            return new Response<ICollection<SchoolEnrollValuesGovernanceDto>>(null, 500, "Não foi possível consultar as matriculas dos alunos. [series]");
        }
    }

    public async Task<Response<ICollection<SchoolEnrollValuesStudentsDto>>> GetStudentsWithSeriesByYearAsync(GetStudentsWithSeriesByYearRequest request)
    {

        var swTotal = Stopwatch.StartNew();
        try
        {
            var swDb = Stopwatch.StartNew();

            var baseQuery = context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == request.Year && x.SchoolInfo.Funcionamento == 1);

            IQueryable<SchoolEnrollValuesStudentsDto>? finalQuery = null;

            if (request.Year > 2022)
            {

                finalQuery = baseQuery
                    .Where(x => x.AtributoId >= 31 && x.AtributoId <= 44)
                    .AsNoTracking()
                    .GroupBy(x => new
                    {
                        x.IdEscolaEnrollValues,
                        x.Ano,
                        x.SchoolInfo.CityInfo.IdMesorregiao,
                        x.SchoolInfo.CityInfo.IdMicrorregiao,
                        x.SchoolInfo.CityInfo.MunicipioId
                    })
                    .Select(g => new SchoolEnrollValuesStudentsDto
                    {
                        EscolaId = g.Key.IdEscolaEnrollValues,
                        Ano = g.Key.Ano,
                        MesorregiaoId = g.Key.IdMesorregiao,
                        MicrorregiaoId = g.Key.IdMicrorregiao,
                        MunicipioId = g.Key.MunicipioId,

                        MatriculaCreche = g.Sum(x => x.AtributoId == 31 ? x.Valor : 0),
                        MatriculaPreEscola = g.Sum(x => x.AtributoId == 32 ? x.Valor : 0),

                        Matricula1Ano = g.Sum(x => x.AtributoId == 33 ? x.Valor : 0),
                        Matricula2Ano = g.Sum(x => x.AtributoId == 34 ? x.Valor : 0),
                        Matricula3Ano = g.Sum(x => x.AtributoId == 35 ? x.Valor : 0),
                        Matricula4Ano = g.Sum(x => x.AtributoId == 36 ? x.Valor : 0),
                        Matricula5Ano = g.Sum(x => x.AtributoId == 37 ? x.Valor : 0),

                        Matricula6Ano = g.Sum(x => x.AtributoId == 38 ? x.Valor : 0),
                        Matricula7Ano = g.Sum(x => x.AtributoId == 39 ? x.Valor : 0),
                        Matricula8Ano = g.Sum(x => x.AtributoId == 40 ? x.Valor : 0),
                        Matricula9Ano = g.Sum(x => x.AtributoId == 41 ? x.Valor : 0),

                        MatriculaMedio1Ano = g.Sum(x => x.AtributoId == 42 ? x.Valor : 0),
                        MatriculaMedio2Ano = g.Sum(x => x.AtributoId == 43 ? x.Valor : 0),
                        MatriculaMedio3Ano = g.Sum(x => x.AtributoId == 44 ? x.Valor : 0)
                    });
            }
            else
            {
                finalQuery = baseQuery
                    .Where(x => (x.AtributoId >= 31 && x.AtributoId <= 44) || (x.AtributoId >= 123 && x.AtributoId <= 125))
                    .AsNoTracking()
                    .GroupBy(x => new
                    {
                        x.IdEscolaEnrollValues,
                        x.Ano,
                        x.SchoolInfo.CityInfo.IdMesorregiao,
                        x.SchoolInfo.CityInfo.IdMicrorregiao,
                        x.SchoolInfo.CityInfo.MunicipioId
                    })
                    .Select(g => new SchoolEnrollValuesStudentsDto
                    {
                        EscolaId = g.Key.IdEscolaEnrollValues,
                        Ano = g.Key.Ano,
                        MesorregiaoId = g.Key.IdMesorregiao,
                        MicrorregiaoId = g.Key.IdMicrorregiao,
                        MunicipioId = g.Key.MunicipioId,

                        MatriculaCreche = g.Sum(x => x.AtributoId == 31 ? x.Valor : 0),
                        MatriculaPreEscola = g.Sum(x => x.AtributoId == 32 ? x.Valor : 0),
                        Matricula1Ano = g.Sum(x => x.AtributoId == 123 ? x.Valor : 0),
                        Matricula2Ano = g.Sum(x => x.AtributoId == 34 ? x.Valor : 0),
                        Matricula3Ano = g.Sum(x => x.AtributoId == 35 ? x.Valor : 0),
                        Matricula4Ano = g.Sum(x => x.AtributoId == 36 ? x.Valor : 0),
                        Matricula5Ano = g.Sum(x => x.AtributoId == 37 ? x.Valor : 0),
                        Matricula6Ano = g.Sum(x => x.AtributoId == 124 ? x.Valor : 0),
                        Matricula7Ano = g.Sum(x => x.AtributoId == 39 ? x.Valor : 0),
                        Matricula8Ano = g.Sum(x => x.AtributoId == 40 ? x.Valor : 0),
                        Matricula9Ano = g.Sum(x => x.AtributoId == 41 ? x.Valor : 0),
                        MatriculaMedio1Ano = g.Sum(x => x.AtributoId == 125 ? x.Valor : 0),
                        MatriculaMedio2Ano = g.Sum(x => x.AtributoId == 43 ? x.Valor : 0),
                        MatriculaMedio3Ano = g.Sum(x => x.AtributoId == 44 ? x.Valor : 0)
                    });
            }

            if (request.Limit.HasValue)
            {
                if (request.Limit.HasValue)
                {
                    finalQuery = finalQuery
                        .OrderBy(x => x.EscolaId)
                        .Take(request.Limit.Value);
                }
            }

            var studentsEnrollment = await finalQuery.ToListAsync();

            swDb.Stop();

            Console.WriteLine($"DB + Materialização: {swDb.ElapsedMilliseconds} ms");
            Console.WriteLine($"Quantidade: {studentsEnrollment.Count}");

            var swSerialize = Stopwatch.StartNew();

            var json = System.Text.Json.JsonSerializer.Serialize(studentsEnrollment);

            swSerialize.Stop();

            Console.WriteLine($"Serialização: {swSerialize.ElapsedMilliseconds} ms");
            Console.WriteLine($"Tamanho JSON: {System.Text.Encoding.UTF8.GetByteCount(json) / 1024.0 / 1024.0:F2} MB");

            swTotal.Stop();
            Console.WriteLine($"TOTAL (até aqui): {swTotal.ElapsedMilliseconds} ms");

            return new Response<ICollection<SchoolEnrollValuesStudentsDto>>(studentsEnrollment, 200, $"Matrículas carregadas com sucesso em {swTotal.ElapsedMilliseconds}ms.");
        }
        catch
        {
            return new Response<ICollection<SchoolEnrollValuesStudentsDto>>(null, 500, "Não foi possível consultar as matriculas dos alunos. [series]");
        }
    }

    public async Task<Response<ICollection<SchoolEnrollValuesTeachersDto>>> GetTeachersWithSeriesByYearAsync(GetTeachersWithSeriesByYearRequest request)
    {
        var swTotal = Stopwatch.StartNew();
        try
        {
            var swDb = Stopwatch.StartNew();


            var query = context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == request.Year
                    && new[] { 83, 84, 86, 87, 88, 94 }.Contains(x.AtributoId))
                .GroupBy(x => new
                {
                    x.IdEscolaEnrollValues,
                    x.Ano,
                    x.SchoolInfo.CityInfo.IdMesorregiao,
                    x.SchoolInfo.CityInfo.IdMicrorregiao,
                    x.SchoolInfo.CityInfo.MunicipioId
                })
                .Select(g => new SchoolEnrollValuesTeachersDto
                {
                    EscolaId = g.Key.IdEscolaEnrollValues,
                    Ano = g.Key.Ano,
                    MesorregiaoId = g.Key.IdMesorregiao,
                    MicrorregiaoId = g.Key.IdMicrorregiao,
                    MunicipioId = g.Key.MunicipioId,

                    ProfessorCreche = g.Sum(x => x.AtributoId == 83 ? x.Valor : 0),
                    ProfessorPreEscola = g.Sum(x => x.AtributoId == 84 ? x.Valor : 0),

                    ProfessorEFIniciais = g.Sum(x => x.AtributoId == 86 ? x.Valor : 0),
                    ProfessorEFFinais = g.Sum(x => x.AtributoId == 87 ? x.Valor : 0),
                    ProfessorMedio = g.Sum(x => x.AtributoId == 88 ? x.Valor : 0),
                    ProfessorEspecial = g.Sum(x => x.AtributoId == 94 ? x.Valor : 0),
                });

            if (request.Limit.HasValue)
            {
                query = query.Take(request.Limit.Value);
            }

            var teachersEnrollment = await query.ToListAsync();

            swDb.Stop();

            Console.WriteLine($"DB + Materialização: {swDb.ElapsedMilliseconds} ms");
            Console.WriteLine($"Quantidade: {teachersEnrollment.Count}");

            var swSerialize = Stopwatch.StartNew();

            var json = System.Text.Json.JsonSerializer.Serialize(teachersEnrollment);

            swSerialize.Stop();

            Console.WriteLine($"Serialização: {swSerialize.ElapsedMilliseconds} ms");
            Console.WriteLine($"Tamanho JSON: {System.Text.Encoding.UTF8.GetByteCount(json) / 1024.0 / 1024.0:F2} MB");

            swTotal.Stop();
            Console.WriteLine($"TOTAL (até aqui): {swTotal.ElapsedMilliseconds} ms");

            return new Response<ICollection<SchoolEnrollValuesTeachersDto>>(teachersEnrollment, 200, $"Matrículas carregadas com sucesso em {swTotal.ElapsedMilliseconds}ms.");
        }
        catch
        {
            return new Response<ICollection<SchoolEnrollValuesTeachersDto>>(null, 500, "Não foi possível consultar os professores. [series]");
        }
    }
}