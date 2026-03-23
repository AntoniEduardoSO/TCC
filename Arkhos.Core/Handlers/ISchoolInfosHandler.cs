using Arkhos.Core.Models;
using Arkhos.Core.Models.Dto;
using Arkhos.Core.Requests.SchoolInfos;
using Arkhos.Core.Responses;

namespace Arkhos.Core.Handlers;
public interface ISchoolInfosHandler
{
    Task<Response<ICollection<SchoolInfoMapDto>>> GetByYearAsync(GetSchoolInfoByYearRequest request);
}