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
        double[] governance_ids = [8, 6, 15, 16, 19, 20, 21];

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
                query = query
                    .OrderBy(x => x.EscolaId)
                    .Take(request.Limit.Value);
            }

            var governanceDtos = await query.ToListAsync();

            return new Response<ICollection<SchoolEnrollValuesGovernanceDto>>(governanceDtos, 200, "Governança carregada com sucesso.");
        }
        catch
        {
            return new Response<ICollection<SchoolEnrollValuesGovernanceDto>>(null, 500, "Não foi possível consultar os dados de governança.");
        }
    }

    public async Task<Response<RegionEnrollmentSummaryDto>> GetRegionEnrollmentSummaryByFilterAsync(GetRegionEnrollmentSummaryByFilterRequest request)
    {
        try
        {
            var query = context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == request.Year && x.SchoolInfo.Funcionamento == 1);

            // Filtros de Região
            if (request.MesorregiaoId.HasValue)
                query = query.Where(x => x.SchoolInfo.CityInfo.IdMesorregiao == request.MesorregiaoId.Value);

            if (request.MicrorregiaoId.HasValue)
                query = query.Where(x => x.SchoolInfo.CityInfo.IdMicrorregiao == request.MicrorregiaoId.Value);

            if (request.MunicipioId.HasValue)
                query = query.Where(x => x.SchoolInfo.CityInfo.MunicipioId == request.MunicipioId.Value);

            if (request.Depedencia.HasValue)
                query = query.Where(x => x.SchoolInfo.Dependencia == request.Depedencia.Value);

            var summaryQuery = query
                .GroupBy(x => new { x.Ano })
                .Select(g => new RegionEnrollmentSummaryDto
                {
                    Ano = g.Key.Ano,
                    MesorregiaoId = request.MesorregiaoId ?? 0,
                    MicrorregiaoId = request.MicrorregiaoId ?? 0,
                    MunicipioId = request.MunicipioId ?? 0,

                    // TOTAIS GERAIS 
                    TotalEscolas = g.Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    TotalEscolasUrbanas = g.Where(x => x.SchoolInfo.Localizacao == 1)
                                           .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    TotalEscolasRurais = g.Where(x => x.SchoolInfo.Localizacao == 2)
                                          .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    // --------- REDE MUNICIPAL (Dependencia == 3) --------- //
                    EscolasMunicipaisTotal = g.Where(x => x.SchoolInfo.Dependencia == 3)
                                              .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    EscolasMunicipaisUrbanas = g.Where(x => x.SchoolInfo.Dependencia == 3 && x.SchoolInfo.Localizacao == 1)
                                                .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    EscolasMunicipaisRurais = g.Where(x => x.SchoolInfo.Dependencia == 3 && x.SchoolInfo.Localizacao == 2)
                                               .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    EscolasMunicipaisComCreche = g.Where(x => x.SchoolInfo.Dependencia == 3 && (x.AtributoId == 31 || x.AtributoId == 32) && x.Valor > 0)
                                                  .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    EscolasMunicipaisComFundamental = g.Where(x => x.SchoolInfo.Dependencia == 3 && ((x.AtributoId >= 33 && x.AtributoId <= 41) || x.AtributoId == 123 || x.AtributoId == 124) && x.Valor > 0)
                                                       .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    EscolasMunicipaisComMedio = g.Where(x => x.SchoolInfo.Dependencia == 3 && ((x.AtributoId >= 42 && x.AtributoId <= 44) || x.AtributoId == 125) && x.Valor > 0)
                                                 .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    // --------- REDE ESTADUAL (Dependencia == 2) --------- //
                    EscolasEstaduaisTotal = g.Where(x => x.SchoolInfo.Dependencia == 2)
                                             .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    EscolasEstaduaisUrbanas = g.Where(x => x.SchoolInfo.Dependencia == 2 && x.SchoolInfo.Localizacao == 1)
                                               .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    EscolasEstaduaisRurais = g.Where(x => x.SchoolInfo.Dependencia == 2 && x.SchoolInfo.Localizacao == 2)
                                              .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    EscolasEstaduaisComCreche = g.Where(x => x.SchoolInfo.Dependencia == 2 && (x.AtributoId == 31 || x.AtributoId == 32) && x.Valor > 0)
                                                 .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    EscolasEstaduaisComFundamental = g.Where(x => x.SchoolInfo.Dependencia == 2 && ((x.AtributoId >= 33 && x.AtributoId <= 41) || x.AtributoId == 123 || x.AtributoId == 124) && x.Valor > 0)
                                                       .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    EscolasEstaduaisComMedio = g.Where(x => x.SchoolInfo.Dependencia == 2 && ((x.AtributoId >= 42 && x.AtributoId <= 44) || x.AtributoId == 125) && x.Valor > 0)
                                                 .Select(x => x.IdEscolaEnrollValues).Distinct().Count(),

                    // --------- MATRÍCULAS GERAIS --------- //
                    MatriculaTotal = g.Sum(x =>
                        (x.AtributoId == 31 || x.AtributoId == 32 || x.AtributoId == 123 ||
                        x.AtributoId == 124 || x.AtributoId == 125) ? x.Valor : 0),

                    MatriculaCreche = g.Sum(x => x.AtributoId == 31 ? x.Valor : 0),
                    MatriculaPreEscola = g.Sum(x => x.AtributoId == 32 ? x.Valor : 0),
                    MatriculaEnsinoFundamentalAI = g.Sum(x => x.AtributoId == 123 ? x.Valor : 0),
                    MatriculaEnsinoFundamentalAF = g.Sum(x => x.AtributoId == 124 ? x.Valor : 0),
                    MatriculaEnsinoMedio = g.Sum(x => x.AtributoId == 125 ? x.Valor : 0),
                    
                    // --------- PROFESSORES --------- //
                    ProfessorCreche = g.Sum(x => x.AtributoId == 83 ? x.Valor : 0),
                    ProfessorPreEscola = g.Sum(x => x.AtributoId == 84 ? x.Valor : 0),
                    ProfessorEFIniciais = g.Sum(x => x.AtributoId == 86 ? x.Valor : 0),
                    ProfessorEFFinais = g.Sum(x => x.AtributoId == 87 ? x.Valor : 0),
                    ProfessorMedio = g.Sum(x => x.AtributoId == 88 ? x.Valor : 0),
                    ProfessorEspecial = g.Sum(x => x.AtributoId == 94 ? x.Valor : 0),

                    // --------- GOVERNANÇA E EQUIPE (Band-aid para 88888) --------- //
                    Psicologo = g.Sum(x => x.AtributoId == 8 ? (x.Valor >= 8888 ? 3 : x.Valor) : 0),
                    Fonaudiologo = g.Sum(x => x.AtributoId == 6 ? (x.Valor >= 8888 ? 3 : x.Valor) : 0),
                    AssistenteSocial = g.Sum(x => x.AtributoId == 15 ? (x.Valor >= 8888 ? 3 : x.Valor) : 0),
                    TradutorLibras = g.Sum(x => x.AtributoId == 16 ? (x.Valor >= 8888 ? 3 : x.Valor) : 0),
                    AssociacaoPaiMestres = g.Sum(x => x.AtributoId == 19 ? (x.Valor >= 8888 ? 1 : x.Valor) : 0),
                    ConselhoEscolar = g.Sum(x => x.AtributoId == 20 ? (x.Valor >= 8888 ? 1 : x.Valor) : 0),
                    GremioEstudantil = g.Sum(x => x.AtributoId == 21 ? (x.Valor >= 8888 ? 1 : x.Valor) : 0)
                });

            var result = await summaryQuery.FirstOrDefaultAsync();

            if (result == null)
                return new Response<RegionEnrollmentSummaryDto>(new RegionEnrollmentSummaryDto { Ano = request.Year }, 200, "Zerar");

            return new Response<RegionEnrollmentSummaryDto>(result, 200, "Resumo regional carregado com sucesso.");
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
        catch
        {
            return new Response<ICollection<RegionEnrollmentSummaryDto>>(null, 500, "Erro ao listar resumos.");
        }
    }

    public async Task<Response<SchoolEnrollmentDetailDto>> GetSchoolEnrollmentDetailByFilterAsync(GetSchoolEnrollmentSummaryByFilterRequest request)
    {
        try
        {
            var detailQuery = context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == request.Year && x.IdEscolaEnrollValues == request.SchoolId)
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

                    // MATRÍCULAS
                    MatriculaCreche = g.Sum(x => x.AtributoId == 31 ? x.Valor : 0),
                    MatriculaPreEscola = g.Sum(x => x.AtributoId == 32 ? x.Valor : 0),
                    MatriculaEnsinoFundamentalAI = g.Sum(x => (x.AtributoId >= 33 && x.AtributoId <= 37) || x.AtributoId == 123 ? x.Valor : 0),
                    MatriculaEnsinoFundamentalAF = g.Sum(x => (x.AtributoId >= 38 && x.AtributoId <= 41) || x.AtributoId == 124 ? x.Valor : 0),
                    MatriculaEnsinoMedio = g.Sum(x => (x.AtributoId >= 42 && x.AtributoId <= 44) || x.AtributoId == 125 ? x.Valor : 0),

                    // PROFESSORES
                    ProfessorCreche = g.Sum(x => x.AtributoId == 83 ? x.Valor : 0),
                    ProfessorPreEscola = g.Sum(x => x.AtributoId == 84 ? x.Valor : 0),
                    ProfessorEFIniciais = g.Sum(x => x.AtributoId == 86 ? x.Valor : 0),
                    ProfessorEFFinais = g.Sum(x => x.AtributoId == 87 ? x.Valor : 0),
                    ProfessorMedio = g.Sum(x => x.AtributoId == 88 ? x.Valor : 0),
                    ProfessorEspecial = g.Sum(x => x.AtributoId == 94 ? x.Valor : 0),

                    // GOVERNANÇA E EQUIPE
                    Psicologo = g.Sum(x => x.AtributoId == 8 ? x.Valor : 0),
                    Fonaudiologo = g.Sum(x => x.AtributoId == 6 ? x.Valor : 0),
                    AssistenteSocial = g.Sum(x => x.AtributoId == 15 ? x.Valor : 0),
                    TradutorLibras = g.Sum(x => x.AtributoId == 16 ? x.Valor : 0),
                    AssociacaoPaiMestres = g.Sum(x => x.AtributoId == 19 ? x.Valor : 0),
                    ConselhoEscolar = g.Sum(x => x.AtributoId == 20 ? x.Valor : 0),
                    GremioEstudantil = g.Sum(x => x.AtributoId == 21 ? x.Valor : 0)
                });

            var result = await detailQuery.FirstOrDefaultAsync();

            if (result == null)
            {
                return new Response<SchoolEnrollmentDetailDto>(null, 404, "Escola não encontrada para o ano informado.");
            }

            return new Response<SchoolEnrollmentDetailDto>(result, 200, "Detalhes da escola carregados com sucesso.");
        }
        catch
        {
            return new Response<SchoolEnrollmentDetailDto>(null, 500, "Erro interno ao consultar os detalhes da escola.");
        }
    }

    public async Task<Response<ICollection<SchoolEnrollValuesStudentsDto>>> GetStudentsWithSeriesByYearAsync(GetStudentsWithSeriesByYearRequest request)
    {
        try
        {
            var baseQuery = context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == request.Year && x.SchoolInfo.Funcionamento == 1);

            IQueryable<SchoolEnrollValuesStudentsDto>? finalQuery = null;

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
                finalQuery = finalQuery
                    .OrderBy(x => x.EscolaId)
                    .Take(request.Limit.Value);
            }

            var studentsEnrollment = await finalQuery.ToListAsync();

            return new Response<ICollection<SchoolEnrollValuesStudentsDto>>(studentsEnrollment, 200, "Matrículas carregadas com sucesso.");
        }
        catch
        {
            return new Response<ICollection<SchoolEnrollValuesStudentsDto>>(null, 500, "Não foi possível consultar as matrículas dos alunos.");
        }
    }

    public async Task<Response<ICollection<SchoolEnrollValuesTeachersDto>>> GetTeachersWithSeriesByYearAsync(GetTeachersWithSeriesByYearRequest request)
    {
        try
        {
            var query = context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == request.Year && new[] { 83, 84, 86, 87, 88, 94 }.Contains(x.AtributoId))
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
                query = query
                    .OrderBy(x => x.EscolaId)
                    .Take(request.Limit.Value);
            }

            var teachersEnrollment = await query.ToListAsync();

            return new Response<ICollection<SchoolEnrollValuesTeachersDto>>(teachersEnrollment, 200, "Professores carregados com sucesso.");
        }
        catch
        {
            return new Response<ICollection<SchoolEnrollValuesTeachersDto>>(null, 500, "Não foi possível consultar os professores.");
        }
    }

}