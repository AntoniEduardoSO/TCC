using Arkhos.Core.Models;
using Arkhos.Core.Models.Dto;
using Arkhos.Core.Requests.SchoolRatings;
using Arkhos.Core.Responses;

namespace Arkhos.Core.Handlers;
public interface ISchoolRatingsHandler
{
    Task<Response<ICollection<SchoolRatingSpendingDto>>> GetByYearAsync(GetSchoolRatingByYearRequest request);

    Task<Response<ICollection<SchoolRatingDropDto>>> GetDropByYearAsync(GetSchoolRatingDropByYearRequest request);
}