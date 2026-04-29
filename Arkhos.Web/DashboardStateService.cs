using Arkhos.Core.Models.Dto.SchoolInfo;

namespace Arkhos.Web;

public class DashboardStateService
{
    public ICollection<SchoolInfoMapDto>? AllSchools { get; private set; }
    public int SelectedYear { get; private set; } = 2025;
    public int? SelectedDependency { get; private set; } = null;
    public bool IsLoading { get; private set; } = false;

    public event Action? OnYearChanged;
    public event Action? OnLoadingChanged;
    public event Action? OnFilterChanged;

    public void SetYear(int year)
    {
        if (SelectedYear == year)
            return;

        SelectedYear = year;
        OnYearChanged?.Invoke();
    }

    public void SetDependency(int? dependency)
    {
        SelectedDependency = dependency;
        OnFilterChanged?.Invoke(); // Dispara a atualização dos cards
    }

    public void SetLoading(bool loading)
    {
        if (IsLoading == loading)
            return;

        IsLoading = loading;
        OnLoadingChanged?.Invoke();
    }

    public void UpdateSchools(ICollection<SchoolInfoMapDto>? schools)
    {
        AllSchools = schools;
    }
}
