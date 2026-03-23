using Arkhos.Core.Models.Dto;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;

namespace Arkhos.Core.Handlers;
public interface ISchoolEnrollValuesHandler
{
    Task<Response<ICollection<SchoolEnrollValuesStudentsDto>>> GetStudentsWithSeriesByYearAsync(GetStudentsWithSeriesByYearRequest request);
    Task<Response<ICollection<SchoolEnrollValuesTeachersDto>>> GetTeachersWithSeriesByYearAsync(GetTeachersWithSeriesByYearRequest request);
}