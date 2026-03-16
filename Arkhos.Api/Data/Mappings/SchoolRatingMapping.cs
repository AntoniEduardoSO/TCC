using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Arkhos.Api.Data.Mappings;

public class SchoolRatingMapping : IEntityTypeConfiguration<SchoolRating>
{
    public void Configure(EntityTypeBuilder<SchoolRating> builder)
    {
        builder.ToTable("school_rating");

        builder.HasKey(x => x.Id);

        builder.Property(x =>x.Id)
        .HasIdentityOptions(startValue:1, incrementBy:1);

        builder.Property(x => x.SchoolInfoId)
        .HasColumnName("id_school_fk");

        builder.Property(x => x.Ano)
        .HasColumnName("ano")
        .HasColumnType("int")
        .IsRequired();

        builder.Property(x => x.AcessibilityRating)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("acessibility_rating");

        builder.Property(x => x.RecreationRating)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("recreation_rating");

        builder.Property(x => x.WellbeingRating)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("wellbeing_rating");

        builder.Property(x => x.HumanSupportRating)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("human_support_rating");
        
        builder.Property(x => x.ManagementRating)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("management_rating");


        builder.Property(x => x.AgeGradeDistortionRating)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("age_grade_distortion_rating");

        builder.Property(x => x.PedagogicalRating)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("pedagogical_rating");
   
        builder.Property(x => x.TeacherStressRating)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("teacher_stress_rating");
        
        builder.Property(x => x.TeacherInstabilityRating)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("teacher_instability_rating");

        builder.Property(x => x.AdministrativeBurdenRating)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("administrative_burden_rating");

        builder.Property(x => x.IdebRating)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("ideb_rating");

        builder.Property(x => x.SaebRating)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("saeb_rating");

        
        builder.Property(x => x.ApprovalRate)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("approval_rate");

        builder.Property(x => x.FailureRate)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("failure_rate");

        builder.Property(x => x.DropoutRate)
            .HasColumnType("numeric(5,4)")
            .HasColumnName("dropout_rate");

        builder.Property(x => x.SpendingPerStudent)
            .HasColumnType("numeric(14,4)")
            .HasColumnName("spending_per_student");

        builder.Property(x => x.SpendingPerTeacher)
            .HasColumnType("numeric(14,4)")
            .HasColumnName("spending_per_teacher");
        
        builder.Property(x => x.PedagogicalSpendingPerStudent)
            .HasColumnType("numeric(14,4)")
            .HasColumnName("pedagogical_spending_per_student");

        builder.Property(x => x.InfrastructureSpendingPerStudent)
            .HasColumnType("numeric(14,4)")
            .HasColumnName("infrastructure_spending_per_student");

        builder.Property(x => x.MealSpendingPerStudent)
            .HasColumnType("numeric(14,4)")
            .HasColumnName("meal_spending_per_student");

        builder.Property(x => x.TransportSpendingPerStudent)
            .HasColumnType("numeric(14,4)")
            .HasColumnName("transport_spending_per_student");

    }
}