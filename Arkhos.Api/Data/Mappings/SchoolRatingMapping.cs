using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Arkhos.Api.Data.Mappings;

public class SchoolRatingMapping : IEntityTypeConfiguration<SchoolRating>
{
    public void Configure(EntityTypeBuilder<SchoolRating> builder)
    {
        builder.ToTable("school_rating");

        builder.HasKey(x => new { x.SchoolInfoId, x.Ano });
        builder.HasIndex(x => x.Ano);


        builder.Property(x => x.SchoolInfoId)
            .HasColumnName("id_escola_fk")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.Ano)
            .HasColumnName("ano")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.AcessibilityRating)
            .HasColumnType("REAL")
            .HasColumnName("acessibility_rating");

        builder.Property(x => x.RecreationRating)
            .HasColumnType("REAL")
            .HasColumnName("recreation_rating");

        builder.Property(x => x.WellbeingRating)
            .HasColumnType("REAL")
            .HasColumnName("wellbeing_rating");

        builder.Property(x => x.HumanSupportRating)
            .HasColumnType("REAL")
            .HasColumnName("human_support_rating");
        
        builder.Property(x => x.ManagementRating)
            .HasColumnType("REAL")
            .HasColumnName("management_rating");

        builder.Property(x => x.AgeGradeDistortionRating)
            .HasColumnType("REAL")
            .HasColumnName("age_grade_distortion_rating");

        builder.Property(x => x.PedagogicalRating)
            .HasColumnType("REAL")
            .HasColumnName("pedagogical_rating");
   
        builder.Property(x => x.TeacherStressRating)
            .HasColumnType("REAL")
            .HasColumnName("teacher_stress_rating");
        
        builder.Property(x => x.TeacherInstabilityRating)
            .HasColumnType("REAL")
            .HasColumnName("teacher_instability_rating");

        builder.Property(x => x.AdministrativeBurdenRating)
            .HasColumnType("REAL")
            .HasColumnName("administrative_burden_rating");

        builder.Property(x => x.IdebRating)
            .HasColumnType("REAL")
            .HasColumnName("ideb_rating");

        builder.Property(x => x.SaebRating)
            .HasColumnType("REAL")
            .HasColumnName("saeb_rating");
        
        builder.Property(x => x.ApprovalRate)
            .HasColumnType("REAL")
            .HasColumnName("approval_rate");

        builder.Property(x => x.FailureRate)
            .HasColumnType("REAL")
            .HasColumnName("failure_rate");

        builder.Property(x => x.DropoutRate)
            .HasColumnType("REAL")
            .HasColumnName("dropout_rate");

        builder.Property(x => x.SpendingPerStudent)
            .HasColumnType("REAL")
            .HasColumnName("spending_per_student");

        builder.Property(x => x.SpendingPerTeacher)
            .HasColumnType("REAL")
            .HasColumnName("spending_per_teacher");
        
        builder.Property(x => x.PedagogicalSpendingPerStudent)
            .HasColumnType("REAL")
            .HasColumnName("pedagogical_spending_per_student");

        builder.Property(x => x.InfrastructureSpendingPerStudent)
            .HasColumnType("REAL")
            .HasColumnName("infrastructure_spending_per_student");

        builder.Property(x => x.MealSpendingPerStudent)
            .HasColumnType("REAL")
            .HasColumnName("meal_spending_per_student");

        builder.Property(x => x.TransportSpendingPerStudent)
            .HasColumnType("REAL")
            .HasColumnName("transport_spending_per_student");

        builder.HasOne(x => x.SchoolInfo)
            .WithOne(x => x.SchoolRating)
            .HasForeignKey<SchoolRating>(x => new { x.SchoolInfoId, x.Ano })
            .HasPrincipalKey<SchoolInfo>(x => new { x.IdEscola, x.Ano });

    }
}