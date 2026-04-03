using Arkhos.Core.Models.Dto.SchoolInfraValues;
using Arkhos.Core.Requests.SchoolInfraValues;
using Arkhos.Core.Responses;

namespace Arkhos.Core.Handlers;
public interface ISchoolInfraValuesHandler
{
    Task<Response<ICollection<SchoolInfraValuesPedagogicalRoomsDto>>> 
        GetPedagogicalRoomsByYearAsync(GetSchoolInfraValuesPedagogicalRoomsByYearRequest request);

    
}