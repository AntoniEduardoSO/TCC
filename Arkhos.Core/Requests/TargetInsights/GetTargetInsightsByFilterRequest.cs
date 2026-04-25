namespace Arkhos.Core.Requests.TargetInsights; 
public class GetTargetInsightsByFilterRequest
{
    public int? Limit {get;set;}
    public int? Year { get; set; }
    public string? Level { get; set; }
    public int? Target { get; set; }   
}