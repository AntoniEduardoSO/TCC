using Arkhos.Core.Models.Dto.CityInfo;
using Arkhos.Core.Requests.CityInfos;
using Arkhos.Core.Responses;

namespace Arkhos.Core.Handlers;
public interface ICityInfosHandler
{
    Task<Response<ICollection<CityInfoMapDto>>> GetByYearAsync(GetCityInfosByYearRequest request);

}