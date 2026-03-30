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
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    municipio_id = table.Column<int>(type: "int", nullable: false),
                    nome_municipio = table.Column<string>(type: "text", nullable: false),
                    ano = table.Column<int>(type: "int", nullable: false),
                    nome_mesorregiao = table.Column<string>(type: "text", nullable: false),
                    id_mesorregiao = table.Column<int>(type: "int", nullable: false),
                    nome_microrregiao = table.Column<string>(type: "text", nullable: false),
                    id_microrregiao = table.Column<int>(type: "int", nullable: false),
                    area_territorial = table.Column<int>(type: "int", nullable: false),
                    populacao_total = table.Column<int>(type: "int", nullable: false),
                    densidade_demografica = table.Column<double>(type: "numeric(5,2)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_city_info", x => x.Id);
                    table.UniqueConstraint("AK_city_info_municipio_id_ano", x => new { x.municipio_id, x.ano });
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
                    tamanho = table.Column<string>(type: "text", nullable: false),
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
                    tamanho = table.Column<string>(type: "text", nullable: false),
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
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    escola_id = table.Column<int>(type: "int", nullable: false),
                    nome_escola = table.Column<string>(type: "text", nullable: false),
                    dependencia = table.Column<short>(type: "smallint", nullable: false),
                    Localizacao = table.Column<short>(type: "smallint", nullable: true),
                    funcionamento = table.Column<short>(type: "smallint", nullable: false),
                    sede = table.Column<int>(type: "int", nullable: true),
                    alocacao = table.Column<short>(type: "smallint", nullable: false),
                    ocupacao = table.Column<short>(type: "smallint", nullable: false),
                    ano = table.Column<int>(type: "int", nullable: false),
                    endereco = table.Column<string>(type: "text", nullable: false),
                    telefone = table.Column<string>(type: "text", nullable: true),
                    lat = table.Column<double>(type: "numeric(9,6)", nullable: true),
                    lon = table.Column<double>(type: "numeric(9,6)", nullable: true),
                    id_municipio_fk = table.Column<int>(type: "int", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_info", x => x.Id);
                    table.UniqueConstraint("AK_school_info_escola_id_ano", x => new { x.escola_id, x.ano });
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
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    ano = table.Column<int>(type: "int", nullable: false),
                    id_escola_fk = table.Column<int>(type: "int", nullable: false),
                    id_atributo = table.Column<int>(type: "int", nullable: false),
                    tipo_atributo = table.Column<string>(type: "text", nullable: false),
                    valor = table.Column<double>(type: "numeric(10,1)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_enroll_values", x => x.Id);
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
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    ano = table.Column<int>(type: "int", nullable: false),
                    id_escola_fk = table.Column<int>(type: "int", nullable: false),
                    id_atributo = table.Column<int>(type: "int", nullable: false),
                    tipo_atributo = table.Column<string>(type: "text", nullable: false),
                    valor = table.Column<double>(type: "numeric(10,1)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_infra_values", x => x.Id);
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
                    Id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:IdentitySequenceOptions", "'1', '1', '', '', 'False', '1'")
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    id_escola_fk = table.Column<int>(type: "int", nullable: false),
                    ano = table.Column<int>(type: "int", nullable: false),
                    acessibility_rating = table.Column<double>(type: "numeric(7,4)", nullable: false),
                    recreation_rating = table.Column<double>(type: "numeric(7,4)", nullable: false),
                    wellbeing_rating = table.Column<double>(type: "numeric(7,4)", nullable: false),
                    human_support_rating = table.Column<double>(type: "numeric(7,4)", nullable: true),
                    management_rating = table.Column<double>(type: "numeric(7,4)", nullable: false),
                    age_grade_distortion_rating = table.Column<double>(type: "numeric(7,4)", nullable: true),
                    pedagogical_rating = table.Column<double>(type: "numeric(7,4)", nullable: false),
                    teacher_stress_rating = table.Column<double>(type: "numeric(7,4)", nullable: false),
                    teacher_instability_rating = table.Column<double>(type: "numeric(7,4)", nullable: false),
                    administrative_burden_rating = table.Column<double>(type: "numeric(7,4)", nullable: false),
                    spending_per_student = table.Column<double>(type: "numeric(14,4)", nullable: false),
                    spending_per_teacher = table.Column<double>(type: "numeric(14,4)", nullable: false),
                    pedagogical_spending_per_student = table.Column<double>(type: "numeric(14,4)", nullable: false),
                    infrastructure_spending_per_student = table.Column<double>(type: "numeric(14,4)", nullable: false),
                    meal_spending_per_student = table.Column<double>(type: "numeric(14,4)", nullable: false),
                    transport_spending_per_student = table.Column<double>(type: "numeric(14,4)", nullable: false),
                    approval_rate = table.Column<double>(type: "numeric(7,4)", nullable: true),
                    failure_rate = table.Column<double>(type: "numeric(7,4)", nullable: true),
                    dropout_rate = table.Column<double>(type: "numeric(7,4)", nullable: true),
                    ideb_rating = table.Column<double>(type: "numeric(7,4)", nullable: true),
                    saeb_rating = table.Column<double>(type: "numeric(7,4)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_rating", x => x.Id);
                    table.ForeignKey(
                        name: "FK_school_rating_school_info_id_escola_fk_ano",
                        columns: x => new { x.id_escola_fk, x.ano },
                        principalTable: "school_info",
                        principalColumns: new[] { "escola_id", "ano" },
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_city_info_municipio_id_ano",
                table: "city_info",
                columns: new[] { "municipio_id", "ano" });

            migrationBuilder.CreateIndex(
                name: "IX_school_enroll_values_id_atributo",
                table: "school_enroll_values",
                column: "id_atributo");

            migrationBuilder.CreateIndex(
                name: "IX_school_enroll_values_id_escola_fk_ano",
                table: "school_enroll_values",
                columns: new[] { "id_escola_fk", "ano" });

            migrationBuilder.CreateIndex(
                name: "IX_school_info_ano",
                table: "school_info",
                column: "ano");

            migrationBuilder.CreateIndex(
                name: "IX_school_info_escola_id_ano",
                table: "school_info",
                columns: new[] { "escola_id", "ano" });

            migrationBuilder.CreateIndex(
                name: "IX_school_info_id_municipio_fk_ano",
                table: "school_info",
                columns: new[] { "id_municipio_fk", "ano" });

            migrationBuilder.CreateIndex(
                name: "IX_school_infra_values_id_atributo",
                table: "school_infra_values",
                column: "id_atributo");

            migrationBuilder.CreateIndex(
                name: "IX_school_infra_values_id_escola_fk_ano",
                table: "school_infra_values",
                columns: new[] { "id_escola_fk", "ano" });

            migrationBuilder.CreateIndex(
                name: "IX_school_rating_ano",
                table: "school_rating",
                column: "ano");

            migrationBuilder.CreateIndex(
                name: "IX_school_rating_id_escola_fk_ano",
                table: "school_rating",
                columns: new[] { "id_escola_fk", "ano" },
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
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
