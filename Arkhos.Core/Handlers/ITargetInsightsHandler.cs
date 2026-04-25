using Arkhos.Core.Models;
using Arkhos.Core.Requests.TargetInsights;
using Arkhos.Core.Responses;

namespace Arkhos.Core.Handlers;
public interface ITargetInsightsHandler
{
    Task<Response<ICollection<TargetInsight>>> GetInsightsByFilterAsync(GetTargetInsightsByFilterRequest request);
}