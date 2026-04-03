namespace Arkhos.Core.Requests.SchoolInfraValues;
public class GetSchoolInfraValuesPedagogicalRoomsByYearRequest : Request
{
    public int Year { get; set; }
    public int? Limit {get;set;}
}