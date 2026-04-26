namespace Arkhos.Core.Requests.SchoolEnrollValues;

public class GetRegionSummariesListRequest : Request
{
    public int Year { get; set; } = 2024;
    public string? ParentLevel { get; set; } 
    public int? ParentId { get; set; }
}