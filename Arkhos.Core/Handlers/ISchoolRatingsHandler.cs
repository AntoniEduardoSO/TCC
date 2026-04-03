using Arkhos.Core.Models.Dto.SchoolRating;
using Arkhos.Core.Requests.SchoolRatings;
using Arkhos.Core.Responses;

namespace Arkhos.Core.Handlers;
public interface ISchoolRatingsHandler
{
    Task<Response<ICollection<SchoolRatingSpendingDto>>> GetSpendingByYearAsync(GetSchoolRatingSpendingByYearRequest request);

    // Task<Re
    Task<Response<ICollection<SchoolRatingDropDto>>> GetDropByYearAsync(GetSchoolRatingDropByYearRequest request);
}