namespace Arkhos.Core.Requests.SchoolEnrollValues;
public class GetStudentsWithSeriesByYearRequest : Request
{
    public int Year { get; set; }
    public int? Limit {get;set;}
}