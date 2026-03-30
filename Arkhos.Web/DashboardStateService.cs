namespace Arkhos.Web;

public class DashboardStateService
{
    public int SelectedYear { get; private set; } = 2024;
    public bool IsLoading { get; private set; } = false;

    public event Action? OnYearChanged;
    public event Action? OnLoadingChanged;

    public void SetYear(int year)
    {
        if (SelectedYear == year)
            return;

        SelectedYear = year;
        OnYearChanged?.Invoke();
    }

    public void SetLoading(bool loading)
    {
        if (IsLoading == loading)
            return;

        IsLoading = loading;
        OnLoadingChanged?.Invoke();
    }
}
