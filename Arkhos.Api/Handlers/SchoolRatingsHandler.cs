using System.Diagnostics;
using Arkhos.Api.Data;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolRating;
using Arkhos.Core.Requests;
using Arkhos.Core.Requests.SchoolRatings;
using Arkhos.Core.Responses;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;

namespace Arkhos.Api.Handlers;

public class SchoolRatingsHandler(AppDbContext context, IMemoryCache cache) : ISchoolRatingsHandler
{
    public async Task<Response<RegionRatingSummaryDto>> GetRegionRatingSummaryAsync(GetRegionSummaryRequest request)
    {
        string cacheKey = $"RatingRegion_{request.Year}_{request.MesorregiaoId}_{request.MunicipioId}_{request.Dependencia}";

        try
        {
            var result = await cache.GetOrCreateAsync(cacheKey, async entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(2);

                var query = context.SchoolRatings.AsNoTracking().Where(x => x.Ano == request.Year);

                if (request.MunicipioId.HasValue) query = query.Where(x => x.SchoolInfo.CityInfo.MunicipioId == request.MunicipioId);
                if (request.Dependencia.HasValue) query = query.Where(x => x.SchoolInfo.Dependencia == request.Dependencia);

                return await query
                    .GroupBy(x => x.Ano)
                    .Select(g => new RegionRatingSummaryDto
                    {
                        Ano = g.Key,
                        AvgSpendingPerStudent = g.Average(x => x.SpendingPerStudent),
                        AvgSpendingPerTeacher = g.Average(x => x.SpendingPerTeacher),
                        AvgInfrastructureSpending = g.Average(x => x.InfrastructureSpendingPerStudent),
                        ApprovalRate = g.Average(x => x.ApprovalRate),
                        FailureRate = g.Average(x => x.FailureRate),
                        DropoutRate = g.Average(x => x.DropoutRate),
                        AvgAccessibilityRating = g.Average(x => x.AcessibilityRating)
                    }).FirstOrDefaultAsync();
            });

            return new Response<RegionRatingSummaryDto>(result);
        }
        catch { return new Response<RegionRatingSummaryDto>(null, 500, "Erro ao processar ratings regionais."); }
    }

    public async Task<Response<SchoolRatingDetailDto>> GetSchoolDetailAsync(int schoolId, int year)
    {
        var result = await context.SchoolRatings
            .AsNoTracking()
            .Where(x => x.SchoolInfoId == schoolId && x.Ano == year)
            .Select(x => new SchoolRatingDetailDto
            {
                EscolaId = x.SchoolInfoId,
                NomeEscola = x.SchoolInfo.NomeEscola,
                Ano = x.Ano,
                SpendingPerStudent = x.SpendingPerStudent,
                ApprovalRate = x.ApprovalRate,
                DropoutRate = x.DropoutRate,
                AccessibilityRating = x.AcessibilityRating
            }).FirstOrDefaultAsync();

        return new Response<SchoolRatingDetailDto>(result);
    }
    
    public async Task<Response<ICollection<SchoolRatingSpendingDto>>> GetSpendingByYearAsync(GetSchoolRatingSpendingByYearRequest request)
    {
        var swTotal = Stopwatch.StartNew();
        try
        {
            var swDb = Stopwatch.StartNew();

            var query = context.SchoolRatings
                .AsNoTracking()
                .Where(x => x.Ano == request.Year)
                .Select(x => new SchoolRatingSpendingDto
                {
                    Ano = x.Ano,
                    SchoolInfoId = x.SchoolInfoId,
                    SpendingPerStudent = x.SpendingPerStudent,
                    SpendingPerTeacher = x.SpendingPerTeacher,
                    PedagogicalSpendingPerStudent = x.PedagogicalSpendingPerStudent,
                    InfrastructureSpendingPerStudent = x.InfrastructureSpendingPerStudent,
                    MealSpendingPerStudent = x.MealSpendingPerStudent,
                    TransportSpendingPerStudent = x.TransportSpendingPerStudent,

                    MesorregiaoId = x.SchoolInfo.CityInfo.IdMesorregiao,
                    MicrorregiaoId = x.SchoolInfo.CityInfo.IdMicrorregiao,
                    MunicipioId = x.SchoolInfo.CityInfo.MunicipioId
                });


            if (request.Limit.HasValue)
            {
                query = query.Take(request.Limit.Value);
            }

            var schoolratings = await query.ToListAsync();

            swDb.Stop();


            Console.WriteLine($"DB + Materialização: {swDb.ElapsedMilliseconds} ms");
            Console.WriteLine($"Quantidade: {schoolratings.Count}");

            var swSerialize = Stopwatch.StartNew();

            var json = System.Text.Json.JsonSerializer.Serialize(schoolratings);

            swSerialize.Stop();

            Console.WriteLine($"Serialização: {swSerialize.ElapsedMilliseconds} ms");
            Console.WriteLine($"Tamanho JSON: {System.Text.Encoding.UTF8.GetByteCount(json) / 1024.0 / 1024.0:F2} MB");

            swTotal.Stop();
            Console.WriteLine($"TOTAL (até aqui): {swTotal.ElapsedMilliseconds} ms");

            return new Response<ICollection<SchoolRatingSpendingDto>>(schoolratings, message: "Retornado com sucesso o schoolratings.");
        }
        catch
        {
            return new Response<ICollection<SchoolRatingSpendingDto>>(null, 500, "Não foi possível consultar os ratings");
        }
    }

    public async Task<Response<ICollection<SchoolRatingDropDto>>> GetDropByYearAsync(GetSchoolRatingDropByYearRequest request)
    {
        var swTotal = Stopwatch.StartNew();
        try
        {
            var swDb = Stopwatch.StartNew();

            var query = context.SchoolRatings
                .AsNoTracking()
                .Where(x => x.Ano == request.Year)
                .Select(x => new SchoolRatingDropDto
                {
                    Ano = x.Ano,
                    SchoolInfoId = x.SchoolInfoId,
                    ApprovalRate = x.ApprovalRate,
                    FailureRate = x.FailureRate,
                    DropoutRate = x.DropoutRate,
                    MesorregiaoId = x.SchoolInfo.CityInfo.IdMesorregiao,
                    MicrorregiaoId = x.SchoolInfo.CityInfo.IdMicrorregiao,
                    MunicipioId = x.SchoolInfo.CityInfo.MunicipioId
                });


            if (request.Limit.HasValue)
                query = query.Take(request.Limit.Value);


            var schooldrop = await query.ToListAsync();

            swDb.Stop();


            Console.WriteLine($"DB + Materialização: {swDb.ElapsedMilliseconds} ms");
            Console.WriteLine($"Quantidade: {schooldrop.Count}");

            var swSerialize = Stopwatch.StartNew();

            var json = System.Text.Json.JsonSerializer.Serialize(schooldrop);

            swSerialize.Stop();

            Console.WriteLine($"Serialização: {swSerialize.ElapsedMilliseconds} ms");
            Console.WriteLine($"Tamanho JSON: {System.Text.Encoding.UTF8.GetByteCount(json) / 1024.0 / 1024.0:F2} MB");

            swTotal.Stop();
            Console.WriteLine($"TOTAL (até aqui): {swTotal.ElapsedMilliseconds} ms");

            return new Response<ICollection<SchoolRatingDropDto>>(schooldrop, message: "Retornado com sucesso o schooldrop.");
        }
        catch
        {
            return new Response<ICollection<SchoolRatingDropDto>>(null, 500, "Não foi possível consultar os dropouts");
        }
    }

    public async Task<Response<ICollection<SchoolRatingMapDto>>> GetRatingByYearAsync(GetSchoolRatingMapByYearRequest request)
    {
        var swTotal = Stopwatch.StartNew();
        try
        {
            var swDb = Stopwatch.StartNew();

            var query = context.SchoolRatings
                .AsNoTracking()
                .Where(x => x.Ano == request.Year)
                .Select(x => new SchoolRatingMapDto
                {
                    Ano = x.Ano,
                    SchoolInfoId = x.SchoolInfoId,
                    RatingAcessibilidade = x.AcessibilityRating,
                    RatingRecreacao = x.RecreationRating,
                    RatingBemEstar = x.WellbeingRating,
                    RatingSuporteHumano = x.HumanSupportRating ?? null,
                    RatingAdministracao = x.ManagementRating,
                    DistorcaoIdadeSerie = x.AgeGradeDistortionRating ?? null,
                    RatingPedagogico = x.PedagogicalRating,
                    RatingStressProfessor = x.TeacherStressRating,
                    RatingInstabilidadeProfessor = x.TeacherInstabilityRating,
                    RatingCaosAdministracao = x.AdministrativeBurdenRating,
                    MesorregiaoId = x.SchoolInfo.CityInfo.IdMesorregiao,
                    MicrorregiaoId = x.SchoolInfo.CityInfo.IdMicrorregiao,
                    MunicipioId = x.SchoolInfo.CityInfo.MunicipioId
                });


            if (request.Limit.HasValue)
                query = query.Take(request.Limit.Value);


            var schoolrating = await query.ToListAsync();

            swDb.Stop();


            Console.WriteLine($"DB + Materialização: {swDb.ElapsedMilliseconds} ms");
            Console.WriteLine($"Quantidade: {schoolrating.Count}");

            var swSerialize = Stopwatch.StartNew();

            var json = System.Text.Json.JsonSerializer.Serialize(schoolrating);

            swSerialize.Stop();

            Console.WriteLine($"Serialização: {swSerialize.ElapsedMilliseconds} ms");
            Console.WriteLine($"Tamanho JSON: {System.Text.Encoding.UTF8.GetByteCount(json) / 1024.0 / 1024.0:F2} MB");

            swTotal.Stop();
            Console.WriteLine($"TOTAL (até aqui): {swTotal.ElapsedMilliseconds} ms");

            return new Response<ICollection<SchoolRatingMapDto>>(schoolrating, message: "Retornado com sucesso o schoolrating.");
        }
        catch
        {
            return new Response<ICollection<SchoolRatingMapDto>>(null, 500, "Não foi possível consultar os schoolratings");
        }
    }
}