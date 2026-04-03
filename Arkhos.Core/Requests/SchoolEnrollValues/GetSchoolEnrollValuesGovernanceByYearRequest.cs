namespace Arkhos.Core.Requests.SchoolEnrollValues;
public class GetSchoolEnrollValuesGovernanceByYearRequest : Request
{
    public int Year { get; set; }
    public int? Limit { get; set; }
}