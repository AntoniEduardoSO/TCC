namespace Arkhos.Core.Models;

public class SchoolRatings
{
    public long Id { get; set; }

    public long SchoolId { get; set; }

    public int Ano {get;set;}

    public double AcessibilityRating { get; set; }

    public double RecreationRating { get; set; }

    public double WellbeingRating {get;set;}

    public double HumanSupportRating {get;set;}

    public double ManagementRating {get;set;}

    public double AgeGradeDistortionRating {get;set;}

    public double PedagogicalRating {get;set;}

    public double TeacherStressRating {get;set;}

    public double TeacherInstabilityRating {get;set;}

    public double AdministrativeBurdenRating {get;set;}

    public double SpendingPerStudent {get;set;}

    public double SpendingPerTeacher {get;set;}

    public double PedagogicalSpendingPerStudent {get;set;}

    public double InfrastructureSpendingPerStudent {get;set;}

    public double MealSpendingPerStudent {get;set;}

    public double TransportSpendingPerStudent {get;set;}

    public double ApprovalRate {get;set;}

    public double FailureRate {get;set;}

    public double DropoutRate {get;set;}

    public double IdebRating { get; set; }

    public double SaebRating { get; set; }
}