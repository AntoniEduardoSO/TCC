namespace Arkhos.Core.Requests.SchoolEnrollValues;
public class GetSchoolEnrollmentSummaryByFilterRequest : Request
{
    public int Year { get; set; }
    public int SchoolId {get;set;}
}