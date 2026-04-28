using Arkhos.Api.Data;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolEnrollValues;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;

namespace Arkhos.Api.Handlers;

public class SchoolEnrollValuesHandler(AppDbContext context, IMemoryCache cache) : ISchoolEnrollValuesHandler
{
    public async Task<Response<ICollection<SchoolEnrollValuesGovernanceDto>>> GetGovernanceByYearAsync(GetSchoolEnrollValuesGovernanceByYearRequest request)
    {
        // CORREÇÃO CRÍTICA: Array de inteiros para compatibilidade com o Postgres
        int[] governance_ids = [8, 6, 15, 16, 19, 20, 21];

        try
        {
            var query = context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == request.Year && governance_ids.Contains(x.AtributoId))
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
                query = query.OrderBy(x => x.EscolaId).Take(request.Limit.Value);
            }

            var governanceDtos = await query.ToListAsync();

            return new Response<ICollection<SchoolEnrollValuesGovernanceDto>>(governanceDtos, 200, "Governança carregada com sucesso.");
        }
        catch (Exception ex)
        {
            return new Response<ICollection<SchoolEnrollValuesGovernanceDto>>(null, 500, $"Erro ao consultar governança: {ex.Message}");
        }
    }

    public async Task<Response<RegionEnrollmentSummaryDto>> GetRegionEnrollmentSummaryByFilterAsync(GetRegionEnrollmentSummaryByFilterRequest request)
    {
        string cacheKey = $"Enrollment_{request.Year}_{request.MesorregiaoId}_{request.MicrorregiaoId}_{request.MunicipioId}_{request.Depedencia}";
        try
        {
            var resultDto = await cache.GetOrCreateAsync(cacheKey, async entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(2); 

                var query = context.SchoolEnrollValues
                    .AsNoTracking()
                    .Where(x => x.Ano == request.Year && x.SchoolInfo.Funcionamento == 1);

                if (request.MesorregiaoId.HasValue) query = query.Where(x => x.SchoolInfo.CityInfo.IdMesorregiao == request.MesorregiaoId.Value);
                if (request.MicrorregiaoId.HasValue) query = query.Where(x => x.SchoolInfo.CityInfo.IdMicrorregiao == request.MicrorregiaoId.Value);
                if (request.MunicipioId.HasValue) query = query.Where(x => x.SchoolInfo.CityInfo.MunicipioId == request.MunicipioId.Value);
                if (request.Depedencia.HasValue) query = query.Where(x => x.SchoolInfo.Dependencia == request.Depedencia.Value);

                var schoolsData = await query
                    .GroupBy(x => new { x.IdEscolaEnrollValues, x.SchoolInfo.Dependencia, x.SchoolInfo.Localizacao })
                    .Select(g => new
                    {
                        IdEscola = g.Key.IdEscolaEnrollValues,
                        Dependencia = g.Key.Dependencia,
                        Localizacao = g.Key.Localizacao,
                        TemCreche = g.Any(x => (x.AtributoId == 31 || x.AtributoId == 32) && x.Valor > 0),
                        TemFundamental = g.Any(x => ((x.AtributoId >= 33 && x.AtributoId <= 41) || x.AtributoId == 123 || x.AtributoId == 124) && x.Valor > 0),
                        TemMedio = g.Any(x => ((x.AtributoId >= 42 && x.AtributoId <= 44) || x.AtributoId == 125) && x.Valor > 0),
                        MatriculaTotal = g.Sum(x => (x.AtributoId == 31 || x.AtributoId == 32 || x.AtributoId == 123 || x.AtributoId == 124 || x.AtributoId == 125) ? x.Valor : 0),
                        MatriculaCreche = g.Sum(x => x.AtributoId == 31 ? x.Valor : 0),
                        MatriculaPreEscola = g.Sum(x => x.AtributoId == 32 ? x.Valor : 0),
                        MatriculaEnsinoFundamentalAI = g.Sum(x => x.AtributoId == 123 ? x.Valor : 0),
                        MatriculaEnsinoFundamentalAF = g.Sum(x => x.AtributoId == 124 ? x.Valor : 0),
                        MatriculaEnsinoMedio = g.Sum(x => x.AtributoId == 125 ? x.Valor : 0),
                        ProfessorCreche = g.Sum(x => x.AtributoId == 83 ? x.Valor : 0),
                        ProfessorPreEscola = g.Sum(x => x.AtributoId == 84 ? x.Valor : 0),
                        ProfessorEFIniciais = g.Sum(x => x.AtributoId == 86 ? x.Valor : 0),
                        ProfessorEFFinais = g.Sum(x => x.AtributoId == 87 ? x.Valor : 0),
                        ProfessorMedio = g.Sum(x => x.AtributoId == 88 ? x.Valor : 0),
                        ProfessorEspecial = g.Sum(x => x.AtributoId == 94 ? x.Valor : 0),
                        Psicologo = g.Sum(x => x.AtributoId == 8 ? (x.Valor >= 8888 ? 3 : x.Valor) : 0),
                        Fonaudiologo = g.Sum(x => x.AtributoId == 6 ? (x.Valor >= 8888 ? 3 : x.Valor) : 0),
                        AssistenteSocial = g.Sum(x => x.AtributoId == 15 ? (x.Valor >= 8888 ? 3 : x.Valor) : 0),
                        TradutorLibras = g.Sum(x => x.AtributoId == 16 ? (x.Valor >= 8888 ? 3 : x.Valor) : 0),
                        AssociacaoPaiMestres = g.Sum(x => x.AtributoId == 19 ? (x.Valor >= 8888 ? 1 : x.Valor) : 0),
                        ConselhoEscolar = g.Sum(x => x.AtributoId == 20 ? (x.Valor >= 8888 ? 1 : x.Valor) : 0),
                        GremioEstudantil = g.Sum(x => x.AtributoId == 21 ? (x.Valor >= 8888 ? 1 : x.Valor) : 0)
                    })
                    .ToListAsync();

                if (!schoolsData.Any()) return new RegionEnrollmentSummaryDto { Ano = request.Year };

                return new RegionEnrollmentSummaryDto
                {
                    Ano = request.Year,
                    MesorregiaoId = request.MesorregiaoId ?? 0,
                    MicrorregiaoId = request.MicrorregiaoId ?? 0,
                    MunicipioId = request.MunicipioId ?? 0,
                    TotalEscolas = schoolsData.Count,
                    TotalEscolasUrbanas = schoolsData.Count(x => x.Localizacao == 1),
                    TotalEscolasRurais = schoolsData.Count(x => x.Localizacao == 2),
                    EscolasMunicipaisTotal = schoolsData.Count(x => x.Dependencia == 3),
                    EscolasMunicipaisUrbanas = schoolsData.Count(x => x.Dependencia == 3 && x.Localizacao == 1),
                    EscolasMunicipaisRurais = schoolsData.Count(x => x.Dependencia == 3 && x.Localizacao == 2),
                    EscolasMunicipaisComCreche = schoolsData.Count(x => x.Dependencia == 3 && x.TemCreche),
                    EscolasMunicipaisComFundamental = schoolsData.Count(x => x.Dependencia == 3 && x.TemFundamental),
                    EscolasMunicipaisComMedio = schoolsData.Count(x => x.Dependencia == 3 && x.TemMedio),
                    EscolasEstaduaisTotal = schoolsData.Count(x => x.Dependencia == 2),
                    EscolasEstaduaisUrbanas = schoolsData.Count(x => x.Dependencia == 2 && x.Localizacao == 1),
                    EscolasEstaduaisRurais = schoolsData.Count(x => x.Dependencia == 2 && x.Localizacao == 2),
                    EscolasEstaduaisComCreche = schoolsData.Count(x => x.Dependencia == 2 && x.TemCreche),
                    EscolasEstaduaisComFundamental = schoolsData.Count(x => x.Dependencia == 2 && x.TemFundamental),
                    EscolasEstaduaisComMedio = schoolsData.Count(x => x.Dependencia == 2 && x.TemMedio),
                    MatriculaTotal = schoolsData.Sum(x => x.MatriculaTotal),
                    MatriculaCreche = schoolsData.Sum(x => x.MatriculaCreche),
                    MatriculaPreEscola = schoolsData.Sum(x => x.MatriculaPreEscola),
                    MatriculaEnsinoFundamentalAI = schoolsData.Sum(x => x.MatriculaEnsinoFundamentalAI),
                    MatriculaEnsinoFundamentalAF = schoolsData.Sum(x => x.MatriculaEnsinoFundamentalAF),
                    MatriculaEnsinoMedio = schoolsData.Sum(x => x.MatriculaEnsinoMedio),
                    ProfessorCreche = schoolsData.Sum(x => x.ProfessorCreche),
                    ProfessorPreEscola = schoolsData.Sum(x => x.ProfessorPreEscola),
                    ProfessorEFIniciais = schoolsData.Sum(x => x.ProfessorEFIniciais),
                    ProfessorEFFinais = schoolsData.Sum(x => x.ProfessorEFFinais),
                    ProfessorMedio = schoolsData.Sum(x => x.ProfessorMedio),
                    ProfessorEspecial = schoolsData.Sum(x => x.ProfessorEspecial),
                    Psicologo = schoolsData.Sum(x => x.Psicologo),
                    Fonaudiologo = schoolsData.Sum(x => x.Fonaudiologo),
                    AssistenteSocial = schoolsData.Sum(x => x.AssistenteSocial),
                    TradutorLibras = schoolsData.Sum(x => x.TradutorLibras),
                    AssociacaoPaiMestres = schoolsData.Sum(x => x.AssociacaoPaiMestres),
                    ConselhoEscolar = schoolsData.Sum(x => x.ConselhoEscolar),
                    GremioEstudantil = schoolsData.Sum(x => x.GremioEstudantil)
                };
            });

            return new Response<RegionEnrollmentSummaryDto>(resultDto, 200, "Resumo regional carregado com sucesso.");
        }
        catch (Exception ex)
        {
            return new Response<RegionEnrollmentSummaryDto>(null, 500, ex.Message);
        }
    }

    public async Task<Response<ICollection<RegionEnrollmentSummaryDto>>> GetRegionSummariesListAsync(GetRegionSummariesListRequest request)
    {
        try
        {
            var query = context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == request.Year && x.SchoolInfo.Funcionamento == 1);

            if (request.ParentLevel == "meso" && request.ParentId.HasValue)
                query = query.Where(x => x.SchoolInfo.CityInfo.IdMesorregiao == request.ParentId.Value);
            else if (request.ParentLevel == "micro" && request.ParentId.HasValue)
                query = query.Where(x => x.SchoolInfo.CityInfo.IdMicrorregiao == request.ParentId.Value);

            var groupedQuery = request.ParentLevel switch
            {
                "meso" => query.GroupBy(x => new { Id = x.SchoolInfo.CityInfo.IdMicrorregiao, x.Ano }),
                "micro" => query.GroupBy(x => new { Id = x.SchoolInfo.CityInfo.MunicipioId, x.Ano }),
                _ => query.GroupBy(x => new { Id = x.SchoolInfo.CityInfo.IdMesorregiao, x.Ano })
            };

            var list = await groupedQuery.Select(g => new RegionEnrollmentSummaryDto
            {
                Ano = g.Key.Ano,
                MesorregiaoId = request.ParentLevel == null ? g.Key.Id : 0,
                MicrorregiaoId = request.ParentLevel == "meso" ? g.Key.Id : 0,
                MunicipioId = request.ParentLevel == "micro" ? g.Key.Id : 0,

                TotalEscolas = g.Select(x => x.IdEscolaEnrollValues).Distinct().Count(),
                TotalEscolasUrbanas = g.Where(x => x.SchoolInfo.Localizacao == 1).Select(x => x.IdEscolaEnrollValues).Distinct().Count(),
                TotalEscolasRurais = g.Where(x => x.SchoolInfo.Localizacao == 2).Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                EscolasMunicipaisComCreche = g.Where(x => x.SchoolInfo.Dependencia == 3 && (x.AtributoId == 31 || x.AtributoId == 32) && x.Valor > 0)
                                                  .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),
                EscolasMunicipaisComFundamental = g.Where(x => x.SchoolInfo.Dependencia == 3 && ((x.AtributoId >= 33 && x.AtributoId <= 41) || x.AtributoId == 123 || x.AtributoId == 124) && x.Valor > 0)
                                                       .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),
                EscolasMunicipaisComMedio = g.Where(x => x.SchoolInfo.Dependencia == 3 && ((x.AtributoId >= 42 && x.AtributoId <= 44) || x.AtributoId == 125) && x.Valor > 0)
                                                 .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                EscolasEstaduaisComCreche = g.Where(x => x.SchoolInfo.Dependencia == 2 && (x.AtributoId == 31 || x.AtributoId == 32) && x.Valor > 0)
                                                 .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),
                EscolasEstaduaisComFundamental = g.Where(x => x.SchoolInfo.Dependencia == 2 && ((x.AtributoId >= 33 && x.AtributoId <= 41) || x.AtributoId == 123 || x.AtributoId == 124) && x.Valor > 0)
                                                       .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),
                EscolasEstaduaisComMedio = g.Where(x => x.SchoolInfo.Dependencia == 2 && ((x.AtributoId >= 42 && x.AtributoId <= 44) || x.AtributoId == 125) && x.Valor > 0)
                                                 .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

            }).ToListAsync();

            return new Response<ICollection<RegionEnrollmentSummaryDto>>(list);
        }
        catch (Exception ex)
        {
            return new Response<ICollection<RegionEnrollmentSummaryDto>>(null, 500, $"Erro ao listar resumos: {ex.Message}");
        }
    }

    public async Task<Response<SchoolEnrollmentDetailDto>> GetSchoolEnrollmentDetailByFilterAsync(int schoolId, int year)
    {
        try
        {
            var result = await context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == year && x.IdEscolaEnrollValues == schoolId)
                .GroupBy(x => new
                {
                    x.IdEscolaEnrollValues,
                    x.SchoolInfo.NomeEscola,
                    x.Ano
                })
                .Select(g => new SchoolEnrollmentDetailDto
                {
                    EscolaId = g.Key.IdEscolaEnrollValues,
                    NomeEscola = g.Key.NomeEscola,
                    Ano = g.Key.Ano,

                    MatriculaCreche = g.Sum(x => x.AtributoId == 31 ? x.Valor : 0),
                    MatriculaPreEscola = g.Sum(x => x.AtributoId == 32 ? x.Valor : 0),
                    MatriculaEnsinoFundamentalAI = g.Sum(x => (x.AtributoId >= 33 && x.AtributoId <= 37) || x.AtributoId == 123 ? x.Valor : 0),
                    MatriculaEnsinoFundamentalAF = g.Sum(x => (x.AtributoId >= 38 && x.AtributoId <= 41) || x.AtributoId == 124 ? x.Valor : 0),
                    MatriculaEnsinoMedio = g.Sum(x => (x.AtributoId >= 42 && x.AtributoId <= 44) || x.AtributoId == 125 ? x.Valor : 0),

                    ProfessorCreche = g.Sum(x => x.AtributoId == 83 ? x.Valor : 0),
                    ProfessorPreEscola = g.Sum(x => x.AtributoId == 84 ? x.Valor : 0),
                    ProfessorEFIniciais = g.Sum(x => x.AtributoId == 86 ? x.Valor : 0),
                    ProfessorEFFinais = g.Sum(x => x.AtributoId == 87 ? x.Valor : 0),
                    ProfessorMedio = g.Sum(x => x.AtributoId == 88 ? x.Valor : 0),
                    ProfessorEspecial = g.Sum(x => x.AtributoId == 94 ? x.Valor : 0),

                    Psicologo = g.Sum(x => x.AtributoId == 8 ? x.Valor : 0),
                    Fonaudiologo = g.Sum(x => x.AtributoId == 6 ? x.Valor : 0),
                    AssistenteSocial = g.Sum(x => x.AtributoId == 15 ? x.Valor : 0),
                    TradutorLibras = g.Sum(x => x.AtributoId == 16 ? x.Valor : 0),
                    AssociacaoPaiMestres = g.Sum(x => x.AtributoId == 19 ? x.Valor : 0),
                    ConselhoEscolar = g.Sum(x => x.AtributoId == 20 ? x.Valor : 0),
                    GremioEstudantil = g.Sum(x => x.AtributoId == 21 ? x.Valor : 0)
                }).FirstOrDefaultAsync();

            if (result == null) return new Response<SchoolEnrollmentDetailDto>(null, 404, "Escola não encontrada para o ano informado.");

            return new Response<SchoolEnrollmentDetailDto>(result, 200, "Detalhes carregados com sucesso.");
        }
        catch (Exception ex)
        {
            return new Response<SchoolEnrollmentDetailDto>(null, 500, $"Erro interno: {ex.Message}");
        }
    }

    public async Task<Response<ICollection<SchoolEnrollValuesStudentsDto>>> GetStudentsWithSeriesByYearAsync(GetStudentsWithSeriesByYearRequest request)
    {
        try
        {
            var baseQuery = context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == request.Year && x.SchoolInfo.Funcionamento == 1);

            IQueryable<SchoolEnrollValuesStudentsDto>? finalQuery;

            if (request.Year > 2022)
            {
                finalQuery = baseQuery
                    .Where(x => x.AtributoId >= 31 && x.AtributoId <= 44)
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
                finalQuery = finalQuery.OrderBy(x => x.EscolaId).Take(request.Limit.Value);
            }

            var studentsEnrollment = await finalQuery.ToListAsync();

            return new Response<ICollection<SchoolEnrollValuesStudentsDto>>(studentsEnrollment, 200, "Matrículas carregadas com sucesso.");
        }
        catch (Exception ex)
        {
            return new Response<ICollection<SchoolEnrollValuesStudentsDto>>(null, 500, $"Erro ao consultar alunos: {ex.Message}");
        }
    }

    public async Task<Response<ICollection<SchoolEnrollValuesTeachersDto>>> GetTeachersWithSeriesByYearAsync(GetTeachersWithSeriesByYearRequest request)
    {
        int[] teacher_ids = [83, 84, 86, 87, 88, 94];

        try
        {
            var query = context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == request.Year && teacher_ids.Contains(x.AtributoId))
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
                query = query.OrderBy(x => x.EscolaId).Take(request.Limit.Value);
            }

            var teachersEnrollment = await query.ToListAsync();

            return new Response<ICollection<SchoolEnrollValuesTeachersDto>>(teachersEnrollment, 200, "Professores carregados com sucesso.");
        }
        catch (Exception ex)
        {
            return new Response<ICollection<SchoolEnrollValuesTeachersDto>>(null, 500, $"Erro ao consultar professores: {ex.Message}");
        }
    }
}