namespace Arkhos.Core.Requests.SchoolRatings;
public class GetSchoolRatingByYearRequest
{
    public int Year { get; set; }

    public int? Limit {get;set;}
}