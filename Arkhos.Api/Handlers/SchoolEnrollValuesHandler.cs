using System.Diagnostics;
using Arkhos.Api.Data;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Arkhos.Api.Handlers;

public class SchoolEnrollValuesHandler(AppDbContext context) : ISchoolEnrollValuesHandler
{
    public async Task<Response<ICollection<SchoolEnrollValuesStudentsDto>>> GetStudentsWithSeriesByYearAsync(GetStudentsWithSeriesByYearRequest request)
    {

        var swTotal = Stopwatch.StartNew();
        try
        {
            var swDb = Stopwatch.StartNew();

            var query = context.SchoolEnrollValues
                .AsNoTracking()
                .Where(x => x.Ano == request.Year && x.AtributoId >= 31 && x.AtributoId <= 44)
                .GroupBy(x => new 
                {
                    x.IdEscolaValues,
                    x.Ano,
                    x.SchoolInfo.CityInfo.IdMesorregiao,
                    x.SchoolInfo.CityInfo.IdMicrorregiao,
                    x.SchoolInfo.CityInfo.MunicipioId
                })
                .Select(g => new SchoolEnrollValuesStudentsDto
                {
                    EscolaId = g.Key.IdEscolaValues,
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

            if (request.Limit.HasValue)
            {
                query = query.Take(request.Limit.Value);
            }

            var studentsEnrollment = await query.ToListAsync();

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
}

/*
31,QT_MAT_INF_CRE,Matrículas Creche,Num,8,QUANTITATIVO ALUNOS
32,QT_MAT_INF_PRE,Matrículas Pré-Escola,Num,8,QUANTITATIVO ALUNOS
33,QT_MAT_FUND_AI_1,Matrículas 1º Ano,Num,8,QUANTITATIVO ALUNOS
34,QT_MAT_FUND_AI_2,Matrículas 2º Ano,Num,8,QUANTITATIVO ALUNOS
35,QT_MAT_FUND_AI_3,Matrículas 3º Ano,Num,8,QUANTITATIVO ALUNOS
36,QT_MAT_FUND_AI_4,Matrículas 4º Ano,Num,8,QUANTITATIVO ALUNOS
37,QT_MAT_FUND_AI_5,Matrículas 5º Ano,Num,8,QUANTITATIVO ALUNOS
38,QT_MAT_FUND_AF_6,Matrículas 6º Ano,Num,8,QUANTITATIVO ALUNOS
39,QT_MAT_FUND_AF_7,Matrículas 7º Ano,Num,8,QUANTITATIVO ALUNOS
40,QT_MAT_FUND_AF_8,Matrículas 8º Ano,Num,8,QUANTITATIVO ALUNOS
41,QT_MAT_FUND_AF_9,Matrículas 9º Ano,Num,8,QUANTITATIVO ALUNOS
42,QT_MAT_MED_PROP_1,Matrículas Médio Prop. 1º Ano,Num,8,QUANTITATIVO ALUNOS
43,QT_MAT_MED_PROP_2,Matrículas Médio Prop. 2º Ano,Num,8,QUANTITATIVO ALUNOS
44,QT_MAT_MED_PROP_3,Matrículas Médio Prop. 3º Ano,Num,8,QUANTITATIVO ALUNOS
*/