using Arkhos.Api.Data;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models;
using Arkhos.Core.Requests.SchoolInfos;
using Arkhos.Core.Responses;
using Microsoft.EntityFrameworkCore;

namespace Arkhos.Api.Handlers;

public class SchoolInfosHandler(AppDbContext context) : ISchoolInfosHandler
{
    public async Task<Response<ICollection<SchoolInfo>>> GetByYearAsync(GetSchoolInfoByYearRequest request)
    {
        try
        {
            var query = context.SchoolInfos.AsNoTracking().Where(x => x.Ano == request.Year);

            var schoolinfos = await query.ToListAsync();

            return new Response<ICollection<SchoolInfo>>(schoolinfos, message: "Retornado com sucesso o schoolinfos.");
        }
        catch
        {
            return new Response<ICollection<SchoolInfo>>(null, 500, "Não foi possível consultar as categorias");
        }
    }
}