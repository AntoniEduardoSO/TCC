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

        builder.Property(x => x.SchoolInfoId).HasColumnName("id_escola_fk").IsRequired();
        builder.Property(x => x.Ano).HasColumnName("ano").IsRequired();
        builder.Property(x => x.AcessibilityRating).HasColumnName("acessibility_rating");
        builder.Property(x => x.RecreationRating).HasColumnName("recreation_rating");
        builder.Property(x => x.WellbeingRating).HasColumnName("wellbeing_rating");
        builder.Property(x => x.HumanSupportRating).HasColumnName("human_support_rating");
        builder.Property(x => x.ManagementRating).HasColumnName("management_rating");
        builder.Property(x => x.AgeGradeDistortionRating).HasColumnName("age_grade_distortion_rating");
        builder.Property(x => x.PedagogicalRating).HasColumnName("pedagogical_rating");
        builder.Property(x => x.TeacherStressRating).HasColumnName("teacher_stress_rating");
        builder.Property(x => x.TeacherInstabilityRating).HasColumnName("teacher_instability_rating");
        builder.Property(x => x.AdministrativeBurdenRating).HasColumnName("administrative_burden_rating");
        builder.Property(x => x.IdebRating).HasColumnName("ideb_rating");
        builder.Property(x => x.SaebRating).HasColumnName("saeb_rating");
        builder.Property(x => x.ApprovalRate).HasColumnName("approval_rate");
        builder.Property(x => x.FailureRate).HasColumnName("failure_rate");
        builder.Property(x => x.DropoutRate).HasColumnName("dropout_rate");
        builder.Property(x => x.SpendingPerStudent).HasColumnName("spending_per_student");
        builder.Property(x => x.SpendingPerTeacher).HasColumnName("spending_per_teacher");
        builder.Property(x => x.PedagogicalSpendingPerStudent).HasColumnName("pedagogical_spending_per_student");
        builder.Property(x => x.InfrastructureSpendingPerStudent).HasColumnName("infrastructure_spending_per_student");
        builder.Property(x => x.MealSpendingPerStudent).HasColumnName("meal_spending_per_student");
        builder.Property(x => x.TransportSpendingPerStudent).HasColumnName("transport_spending_per_student");

        builder.HasOne(x => x.SchoolInfo)
            .WithOne(x => x.SchoolRating)
            .HasForeignKey<SchoolRating>(x => new { x.SchoolInfoId, x.Ano })
            .HasPrincipalKey<SchoolInfo>(x => new { x.IdEscola, x.Ano });
    }
}