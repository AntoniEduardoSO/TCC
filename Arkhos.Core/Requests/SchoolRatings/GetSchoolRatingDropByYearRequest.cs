namespace Arkhos.Core.Requests.SchoolRatings;
public class GetSchoolRatingDropByYearRequest : Request
{
    public int Year { get; set; }
    public int? Limit { get; set; }
}