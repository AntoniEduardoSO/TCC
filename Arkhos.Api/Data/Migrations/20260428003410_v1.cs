using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace Arkhos.Api.Migrations
{
    /// <inheritdoc />
    public partial class v1 : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "city_info",
                columns: table => new
                {
                    municipio_id = table.Column<int>(type: "integer", nullable: false),
                    ano = table.Column<int>(type: "integer", nullable: false),
                    nome_municipio = table.Column<string>(type: "text", nullable: false),
                    nome_mesorregiao = table.Column<string>(type: "text", nullable: false),
                    id_mesorregiao = table.Column<int>(type: "integer", nullable: false),
                    nome_microrregiao = table.Column<string>(type: "text", nullable: false),
                    id_microrregiao = table.Column<int>(type: "integer", nullable: false),
                    area_territorial = table.Column<int>(type: "integer", nullable: false),
                    populacao_total = table.Column<int>(type: "integer", nullable: false),
                    densidade_demografica = table.Column<double>(type: "double precision", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_city_info", x => new { x.municipio_id, x.ano });
                });

            migrationBuilder.CreateTable(
                name: "city_transparency_portal",
                columns: table => new
                {
                    id = table.Column<string>(type: "text", nullable: false),
                    municipio_id_fk = table.Column<int>(type: "integer", nullable: false),
                    data = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    valor = table.Column<double>(type: "double precision", nullable: true),
                    credor = table.Column<string>(type: "text", nullable: true),
                    elemento_despesa = table.Column<string>(type: "text", nullable: true),
                    detalhe = table.Column<string>(type: "text", nullable: true),
                    eixo = table.Column<string>(type: "text", nullable: false),
                    macro = table.Column<string>(type: "text", nullable: false),
                    micro = table.Column<string>(type: "text", nullable: false),
                    portal_origem = table.Column<int>(type: "integer", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_city_transparency_portal", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "insights",
                columns: table => new
                {
                    id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    axis = table.Column<string>(type: "text", nullable: false),
                    level = table.Column<string>(type: "text", nullable: false),
                    ano = table.Column<int>(type: "integer", nullable: false),
                    tipo_insight = table.Column<string>(type: "text", nullable: false),
                    titulo = table.Column<string>(type: "text", nullable: false),
                    valor_destaque = table.Column<string>(type: "text", nullable: false),
                    descricao = table.Column<string>(type: "text", nullable: false),
                    recomendacao = table.Column<string>(type: "text", nullable: false),
                    valor_baseline = table.Column<double>(type: "double precision", nullable: false),
                    id_alvo = table.Column<int>(type: "integer", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_insights", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "school_enroll_dict",
                columns: table => new
                {
                    id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    variavel = table.Column<string>(type: "text", nullable: false),
                    descricao = table.Column<string>(type: "text", nullable: false),
                    tipo = table.Column<string>(type: "text", nullable: false),
                    tamanho = table.Column<int>(type: "integer", nullable: false),
                    grupo = table.Column<string>(type: "text", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_enroll_dict", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "school_infra_dict",
                columns: table => new
                {
                    id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    variavel = table.Column<string>(type: "text", nullable: false),
                    descricao = table.Column<string>(type: "text", nullable: false),
                    tipo = table.Column<string>(type: "text", nullable: false),
                    tamanho = table.Column<int>(type: "integer", nullable: false),
                    grupo = table.Column<string>(type: "text", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_infra_dict", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "school_info",
                columns: table => new
                {
                    escola_id = table.Column<int>(type: "integer", nullable: false),
                    ano = table.Column<int>(type: "integer", nullable: false),
                    nome_escola = table.Column<string>(type: "text", nullable: false),
                    dependencia = table.Column<short>(type: "smallint", nullable: false),
                    localizacao = table.Column<short>(type: "smallint", nullable: true),
                    funcionamento = table.Column<short>(type: "smallint", nullable: false),
                    sede = table.Column<int>(type: "integer", nullable: true),
                    alocacao = table.Column<short>(type: "smallint", nullable: false),
                    ocupacao = table.Column<short>(type: "smallint", nullable: false),
                    endereco = table.Column<string>(type: "text", nullable: true),
                    telefone = table.Column<string>(type: "text", nullable: true),
                    lat = table.Column<double>(type: "double precision", nullable: true),
                    lon = table.Column<double>(type: "double precision", nullable: true),
                    id_municipio_fk = table.Column<int>(type: "integer", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_info", x => new { x.escola_id, x.ano });
                    table.ForeignKey(
                        name: "FK_school_info_city_info_id_municipio_fk_ano",
                        columns: x => new { x.id_municipio_fk, x.ano },
                        principalTable: "city_info",
                        principalColumns: new[] { "municipio_id", "ano" },
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "school_enroll_values",
                columns: table => new
                {
                    ano = table.Column<int>(type: "integer", nullable: false),
                    id_escola_fk = table.Column<int>(type: "integer", nullable: false),
                    id_atributo = table.Column<int>(type: "integer", nullable: false),
                    tipo_atributo = table.Column<string>(type: "text", nullable: false),
                    valor = table.Column<double>(type: "double precision", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_enroll_values", x => new { x.ano, x.id_escola_fk, x.id_atributo });
                    table.ForeignKey(
                        name: "FK_school_enroll_values_school_enroll_dict_id_atributo",
                        column: x => x.id_atributo,
                        principalTable: "school_enroll_dict",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_school_enroll_values_school_info_id_escola_fk_ano",
                        columns: x => new { x.id_escola_fk, x.ano },
                        principalTable: "school_info",
                        principalColumns: new[] { "escola_id", "ano" },
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "school_infra_values",
                columns: table => new
                {
                    ano = table.Column<int>(type: "integer", nullable: false),
                    id_escola_fk = table.Column<int>(type: "integer", nullable: false),
                    id_atributo = table.Column<int>(type: "integer", nullable: false),
                    tipo_atributo = table.Column<string>(type: "text", nullable: false),
                    valor = table.Column<double>(type: "double precision", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_infra_values", x => new { x.ano, x.id_escola_fk, x.id_atributo });
                    table.ForeignKey(
                        name: "FK_school_infra_values_school_info_id_escola_fk_ano",
                        columns: x => new { x.id_escola_fk, x.ano },
                        principalTable: "school_info",
                        principalColumns: new[] { "escola_id", "ano" },
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_school_infra_values_school_infra_dict_id_atributo",
                        column: x => x.id_atributo,
                        principalTable: "school_infra_dict",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "school_rating",
                columns: table => new
                {
                    id_escola_fk = table.Column<int>(type: "integer", nullable: false),
                    ano = table.Column<int>(type: "integer", nullable: false),
                    acessibility_rating = table.Column<double>(type: "double precision", nullable: false),
                    recreation_rating = table.Column<double>(type: "double precision", nullable: false),
                    wellbeing_rating = table.Column<double>(type: "double precision", nullable: false),
                    human_support_rating = table.Column<double>(type: "double precision", nullable: true),
                    management_rating = table.Column<double>(type: "double precision", nullable: false),
                    age_grade_distortion_rating = table.Column<double>(type: "double precision", nullable: true),
                    pedagogical_rating = table.Column<double>(type: "double precision", nullable: false),
                    teacher_stress_rating = table.Column<double>(type: "double precision", nullable: false),
                    teacher_instability_rating = table.Column<double>(type: "double precision", nullable: false),
                    administrative_burden_rating = table.Column<double>(type: "double precision", nullable: false),
                    spending_per_student = table.Column<double>(type: "double precision", nullable: false),
                    spending_per_teacher = table.Column<double>(type: "double precision", nullable: false),
                    pedagogical_spending_per_student = table.Column<double>(type: "double precision", nullable: false),
                    infrastructure_spending_per_student = table.Column<double>(type: "double precision", nullable: false),
                    meal_spending_per_student = table.Column<double>(type: "double precision", nullable: false),
                    transport_spending_per_student = table.Column<double>(type: "double precision", nullable: false),
                    approval_rate = table.Column<double>(type: "double precision", nullable: true),
                    failure_rate = table.Column<double>(type: "double precision", nullable: true),
                    dropout_rate = table.Column<double>(type: "double precision", nullable: true),
                    ideb_rating = table.Column<double>(type: "double precision", nullable: true),
                    saeb_rating = table.Column<double>(type: "double precision", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_rating", x => new { x.id_escola_fk, x.ano });
                    table.ForeignKey(
                        name: "FK_school_rating_school_info_id_escola_fk_ano",
                        columns: x => new { x.id_escola_fk, x.ano },
                        principalTable: "school_info",
                        principalColumns: new[] { "escola_id", "ano" },
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_city_info_ano",
                table: "city_info",
                column: "ano");

            migrationBuilder.CreateIndex(
                name: "IX_city_transparency_portal_municipio_id_fk",
                table: "city_transparency_portal",
                column: "municipio_id_fk");

            migrationBuilder.CreateIndex(
                name: "IX_insights_ano",
                table: "insights",
                column: "ano");

            migrationBuilder.CreateIndex(
                name: "IX_insights_id_alvo",
                table: "insights",
                column: "id_alvo");

            migrationBuilder.CreateIndex(
                name: "IX_school_enroll_dict_variavel",
                table: "school_enroll_dict",
                column: "variavel");

            migrationBuilder.CreateIndex(
                name: "IX_school_enroll_values_id_atributo",
                table: "school_enroll_values",
                column: "id_atributo");

            migrationBuilder.CreateIndex(
                name: "IX_school_enroll_values_id_escola_fk",
                table: "school_enroll_values",
                column: "id_escola_fk");

            migrationBuilder.CreateIndex(
                name: "IX_school_enroll_values_id_escola_fk_ano",
                table: "school_enroll_values",
                columns: new[] { "id_escola_fk", "ano" });

            migrationBuilder.CreateIndex(
                name: "IX_school_info_ano",
                table: "school_info",
                column: "ano");

            migrationBuilder.CreateIndex(
                name: "IX_school_info_id_municipio_fk",
                table: "school_info",
                column: "id_municipio_fk");

            migrationBuilder.CreateIndex(
                name: "IX_school_info_id_municipio_fk_ano",
                table: "school_info",
                columns: new[] { "id_municipio_fk", "ano" });

            migrationBuilder.CreateIndex(
                name: "IX_school_infra_dict_variavel",
                table: "school_infra_dict",
                column: "variavel");

            migrationBuilder.CreateIndex(
                name: "IX_school_infra_values_id_atributo",
                table: "school_infra_values",
                column: "id_atributo");

            migrationBuilder.CreateIndex(
                name: "IX_school_infra_values_id_escola_fk",
                table: "school_infra_values",
                column: "id_escola_fk");

            migrationBuilder.CreateIndex(
                name: "IX_school_infra_values_id_escola_fk_ano",
                table: "school_infra_values",
                columns: new[] { "id_escola_fk", "ano" });

            migrationBuilder.CreateIndex(
                name: "IX_school_rating_ano",
                table: "school_rating",
                column: "ano");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "city_transparency_portal");

            migrationBuilder.DropTable(
                name: "insights");

            migrationBuilder.DropTable(
                name: "school_enroll_values");

            migrationBuilder.DropTable(
                name: "school_infra_values");

            migrationBuilder.DropTable(
                name: "school_rating");

            migrationBuilder.DropTable(
                name: "school_enroll_dict");

            migrationBuilder.DropTable(
                name: "school_infra_dict");

            migrationBuilder.DropTable(
                name: "school_info");

            migrationBuilder.DropTable(
                name: "city_info");
        }
    }
}
