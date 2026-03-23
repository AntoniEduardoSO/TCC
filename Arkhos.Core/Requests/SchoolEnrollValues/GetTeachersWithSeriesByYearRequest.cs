namespace Arkhos.Core.Requests.SchoolEnrollValues;
public class GetTeachersWithSeriesByYearRequest : Request
{
    public int Year { get; set; }
    public int? Limit { get; set; }
}