namespace Arkhos.Core.Requests.SchoolInfos;
public class GetSchoolInfoByYearRequest : Request
{
    public int Year { get; set; }
    public int? Dependencia { get; set; }
    public int? Limit {get;set;}
}