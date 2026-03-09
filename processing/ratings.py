import pandas as pd
import numpy as np
import os

def save_incremental(df, filepath):
    file_exists = os.path.exists(filepath)

    df.to_csv(filepath, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')

def filter_financial_source(df_fin_city, year):
    df_fin_city_year = df_fin_city[df_fin_city["ano"] == year]

    portals = df_fin_city_year["portal_origem"].unique()

    if any(p != 11 for p in portals):
        df_fin_city_year = df_fin_city_year[df_fin_city_year["portal_origem"] != 11]
    else:
        df_fin_city_year = df_fin_city_year[df_fin_city_year["portal_origem"] == 11]
    
    return df_fin_city_year

def build_school_weight(df_enroll_wide, students_by_school):

    teacher_cols = [
        'QT_DOC_INF',
        'QT_DOC_FUND',
        'QT_DOC_MED',
        'QT_DOC_ESP'
    ]

    teachers_by_school = df_enroll_wide[teacher_cols].sum(axis=1)

    weight = students_by_school + (teachers_by_school * 5)

    return weight

HUMAN_SUPPORT_IDS = [
    6,7,8,15
]

def build_support_staff_by_school_map(df_enroll_wide):

    support_cols = [
        'QT_PROF_PSICOLOGO',
        'QT_PROF_ASSIST_SOCIAL',
        'QT_PROF_FONAUDIOLOGO',
        'QT_PROF_NUTRICIONISTA'
    ]

    df = df_enroll_wide.reindex(columns=support_cols, fill_value=0)

    return df.sum(axis=1).to_dict()

MATRICULA_IDS = [
    31, 32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49
]

def build_students_map(df_enroll_wide):

    student_cols = [
        'QT_MAT_INF_CRE',
        'QT_MAT_INF_PRE',
        'QT_MAT_FUND_AI_1',
        'QT_MAT_FUND_AI_2',
        'QT_MAT_FUND_AI_3',
        'QT_MAT_FUND_AI_4',
        'QT_MAT_FUND_AI_5',
        'QT_MAT_FUND_AF_6',
        'QT_MAT_FUND_AF_7',
        'QT_MAT_FUND_AF_8',
        'QT_MAT_FUND_AF_9',
        'QT_MAT_MED_PROP_1',
        'QT_MAT_MED_PROP_2',
        'QT_MAT_MED_PROP_3',
        'QT_MAT_ESP'
    ]

    cols_existentes = [c for c in student_cols if c in df_enroll_wide.columns]

    return df_enroll_wide[cols_existentes].sum(axis=1)

def get_acessible_rating( df_infra_wide, active_schools_ids, df_fin_city, students_by_school, year):

    df_fin_city_year = filter_financial_source(df_fin_city, year)

    acessible_cols = [
        'QT_SALAS_UTILIZADAS',
        'QT_SALAS_UTILIZADAS_ACESSIVEIS',
        'IN_BANHEIRO_PNE',
        'IN_SALA_ATENDIMENTO_ESPECIAL',
        'IN_ACESSIBILIDADE_CORRIMAO',
        'IN_ACESSIBILIDADE_PISOS_TATEIS',
        'IN_ACESSIBILIDADE_VAO_LIVRE',
        'IN_ACESSIBILIDADE_RAMPAS',
        'IN_ACESSIBILIDADE_SINAL_TATIL'
    ]

    infra_spending = df_fin_city_year[
        (df_fin_city_year["eixo"] == "Infraestrutura Escolar") &
        (df_fin_city_year["macro"].isin([
            "Obras",
            "Manutenção",
            "Infraestrutura Física"
        ]))
    ]

    infra_spending_total = infra_spending["valor"].sum()

    total_students_city = students_by_school.sum()

    ratings_map = {}

    for school_id in active_schools_ids:

        if school_id not in df_infra_wide.index:
            continue

        school_data = df_infra_wide.loc[school_id]

        qnt_room = school_data.get('QT_SALAS_UTILIZADAS', 0)
        qnt_acessible_room = school_data.get('QT_SALAS_UTILIZADAS_ACESSIVEIS', 0)

        sum_acessibility = sum(
            school_data.get(col, 0)
            for col in acessible_cols[2:]
        )

        ratio_rooms = (
            qnt_acessible_room / qnt_room
            if qnt_room > 0 else 0
        )

        infra_structure_score = (
            (ratio_rooms*2 + sum_acessibility)
        ) / len(acessible_cols)

        students_school = students_by_school.get(school_id, 0)

        share = (
            students_school / total_students_city
            if total_students_city > 0 else 0
        )

        infra_spending_school = infra_spending_total * share

        infra_spending_score = min(
            infra_spending_school / 2000,
            1
        )

        rating = round(
            0.8 * infra_structure_score +
            0.2 * infra_spending_score,
            2
        )

        ratings_map[school_id] = rating

    return pd.Series(ratings_map)
    
def get_recreation_rating(df_infra_wide, active_schools_ids, df_fin_city, students_by_school, year):
    recreation_cols = [
        'QT_SALAS_UTILIZADAS', 'QT_SALAS_UTILIZA_CLIMATIZADAS',
        'IN_TERREIRAO', 'IN_AREA_PLANTIO',
        'IN_PATIO_COBERTO', 'IN_PATIO_DESCOBERTO',
        'IN_PARQUE_INFANTIL', 'IN_PISCINA',
        'IN_QUADRA_ESPORTES'
    ]

    df_fin_city_year = filter_financial_source(df_fin_city, year)

    infra_spending = df_fin_city_year[
        (df_fin_city_year["eixo"] == "Infraestrutura Escolar") &
        (df_fin_city_year["macro"].isin([
            "Obras",
            "Manutenção",
            "Infraestrutura Física"
        ]))
    ]

    infra_spending_total = infra_spending["valor"].sum()

    total_students_city = students_by_school.sum()
    
    ratings_map = {}
    
    for school_id in active_schools_ids:

        if school_id not in df_infra_wide.index: 
            continue

        
        school_data = df_infra_wide.loc[school_id]
        
        qnt_room = school_data.get('QT_SALAS_UTILIZADAS', 0)
        qnt_air_conditioned_room = school_data.get('QT_SALAS_UTILIZA_CLIMATIZADAS', 0)
        
        ratio_room = (qnt_air_conditioned_room / qnt_room) if qnt_room > 0 else 0
        
        sum_recreation = sum(
            school_data.get(col, 0)
            for col in recreation_cols[2:]
        )

        recreation_structure_score = (
            ratio_room + sum_recreation
        ) / len(recreation_cols)

        students_school = students_by_school.get(school_id, 0)

        share = (
            students_school / total_students_city
            if total_students_city > 0 else 0
        )

        infra_spending_school = infra_spending_total * share

        infra_spending_score = min(
            infra_spending_school / 2000,
            1
        )
        
        rating = round(
            0.8 * recreation_structure_score +
            0.2 * infra_spending_score,
            2
        )
        
        ratings_map[school_id] = rating
    
    return pd.Series(ratings_map)

def get_wellbeing_rating(df_infra_wide, active_schools_ids, df_fin_city, students_by_school, year):

    wellbeing_cols = [
        'IN_AGUA_POTAVEL', 'IN_ALIMENTACAO',
        'IN_COZINHA', 'IN_REFEITORIO', 
        'IN_ESGOTO_REDE_PUBLICA', 'IN_ENERGIA_REDE_PUBLICA',
        'IN_LIXO_SERVICO_COLETA'
    ]

    df_fin_city_year = filter_financial_source(df_fin_city, year)

    wellbeing_spending = df_fin_city_year[
        (
            (df_fin_city_year["eixo"] == "Infraestrutura Escolar") &
            (df_fin_city_year["macro"].isin([
                "Utilidades",
                "Manutenção"
            ]))
        )
        |
        (
            df_fin_city_year["eixo"] == "Alimentação Escolar"
        )
    ]

    wellbeing_spending_total = wellbeing_spending["valor"].sum()

    total_students_city = students_by_school.sum()

    ratings_map = {}

    for school_id in active_schools_ids:

        if school_id not in df_infra_wide.index:
            continue

        school_data = df_infra_wide.loc[school_id]

        sum_wellbeing = sum(
            school_data.get(col, 0)
            for col in wellbeing_cols
        )

        wellbeing_structure_score = sum_wellbeing / len(wellbeing_cols)

        students_school = students_by_school.get(school_id, 0)

        share = (
            students_school / total_students_city
            if total_students_city > 0 else 0
        )

        wellbeing_spending_school = wellbeing_spending_total * share

        wellbeing_spending_score = min(
            wellbeing_spending_school / 1500,
            1
        )

        rating = round(
            0.8 * wellbeing_structure_score +
            0.2 * wellbeing_spending_score,
            2
        )

        ratings_map[school_id] = rating

    return pd.Series(ratings_map)
        
def get_human_support_rating(df_enroll_wide, active_schools_ids, df_fin_city, students_by_school, support_staff_by_school, year):
    support_staff_cols = [
        'QT_PROF_PSICOLOGO', 'QT_PROF_ASSIST_SOCIAL',
        'QT_PROF_FONAUDIOLOGO', 'QT_PROF_NUTRICIONISTA'
    ]

    df_fin_city_year = filter_financial_source(df_fin_city, year)

    human_spending = df_fin_city_year[
        (df_fin_city_year["eixo"] == "Pessoal") &
        (df_fin_city_year["macro"] == "Magistério/Docentes")
    ]

    human_spending_total = human_spending["valor"].sum()

    total_students_city = students_by_school.sum()
    
    rating_maps = {}
    
    for school_id in active_schools_ids:

        if school_id not in df_enroll_wide.index:
            continue

        school_data = df_enroll_wide.loc[school_id]

        sum_support = sum(
            school_data.get(col, 0)
            for col in support_staff_cols
        )

        support_structure_score = min(sum_support / len(support_staff_cols), 1)

        staff_school = support_staff_by_school.get(school_id, 0)
        students_school = students_by_school.get(school_id, 0)

        staff_ratio = (
            staff_school / students_school
            if students_school > 0 else 0
        )

        staff_ratio_score = min(staff_ratio * 200, 1)

        share = (
            students_school / total_students_city
            if total_students_city > 0 else 0
        )

        human_spending_school = human_spending_total * share

        human_spending_score = min(
            human_spending_school / 3000,
            1
        )

        rating = round(
            0.5 * support_structure_score +
            0.3 * staff_ratio_score +
            0.2 * human_spending_score,
            2
        )
        
        rating_maps[school_id] = rating

    return pd.Series(rating_maps)

def get_management_rating(df_enroll_wide, active_schools_ids):
    management_cols = ['IN_ORGAO_ASS_PAIS', 'IN_ORGAO_CONSELHO_ESCOLAR', 'IN_ORGAO_GREMIO_ESTUDANTIL']
    
    rating_maps = {}
    
    for school_id in active_schools_ids:

        if school_id not in df_enroll_wide.index: 
            continue


        school_data = df_enroll_wide.loc[school_id]

        soma = sum([school_data.get(col, 0) for col in management_cols])
        
        rating = soma / len(management_cols)
        
        rating_maps[school_id] = round(rating, 2)

    return pd.Series(rating_maps)

def get_age_grade_distortion(df_enroll_wide, active_schools_ids):
    distortion_cols = [
        'QT_MAT_BAS_15_17', 'QT_MAT_FUND_AF_6',
        'QT_MAT_FUND_AF_7', 'QT_MAT_FUND_AF_8',
        'QT_MAT_FUND_AF_9']
    
    rating_maps = {}
    
    for school_id in active_schools_ids:

        if school_id not in df_enroll_wide.index: 
            continue


        school_data = df_enroll_wide.loc[school_id]
        
        total_15_17 = school_data.get(distortion_cols[0], 0)

        soma_distortion = sum([school_data.get(col, 0) for col in distortion_cols[1:]])

        ratio = soma_distortion / total_15_17 if total_15_17 > 0 else 0

        score = 1 - min(ratio, 1)
        
        rating_maps[school_id] = round(score, 2)

    return pd.Series(rating_maps)

def get_pedagogical_rating(df_infra_wide, active_schools_ids, df_fin_city, students_by_school, year):
    pedagogical_cols = [
        'IN_BIBLIOTECA_SALA_LEITURA', 'IN_LABORATORIO_INFORMATICA',
        'IN_LABORATORIO_CIENCIAS', 'IN_BANDA_LARGA',
        'IN_INTERNET_ALUNOS', 'IN_MATERIAL_PED_JOGOS'
        ]
    
    df_fin_city_year = filter_financial_source(df_fin_city, year)

    pedagogical_spending = df_fin_city_year[
        (
            (df_fin_city_year["eixo"] == "Infraestrutura Escolar") &
            (df_fin_city_year["macro"].isin([
                "Tecnologia Educacional",
                "Equipamentos"
            ]))
        )
        |
        (
            df_fin_city_year["eixo"] == "Recursos Pedagógicos"
        )
    ]

    pedagogical_spending_total = pedagogical_spending["valor"].sum()

    total_students_city = students_by_school.sum()

    rating_maps = {}
    
    for school_id in active_schools_ids:

        if school_id not in df_infra_wide.index: 
            continue

        school_data = df_infra_wide.loc[school_id]

        sum_pedagogical = sum(
            school_data.get(col, 0)
            for col in pedagogical_cols
        )

        pedagogical_structure_score = sum_pedagogical / len(pedagogical_cols)

        students_school = students_by_school.get(school_id, 0)

        share = (
            students_school / total_students_city
            if total_students_city > 0 else 0
        )

        pedagogical_spending_school = pedagogical_spending_total * share

        pedagogical_spending_score = min(
            pedagogical_spending_school / 2000,
            1
        )

        rating = round(
            0.75 * pedagogical_structure_score +
            0.25 * pedagogical_spending_score,
            2
        )

        rating_maps[school_id] = rating

    return pd.Series(rating_maps)

def get_teacher_stress_rating(df_enroll_wide, active_schools_ids, df_fin_city, students_by_school, year):

    teacher_cols = [
        'QT_DOC_INF',
        'QT_DOC_FUND',
        'QT_DOC_MED',
        'QT_DOC_ESP'
    ]

    class_cols = [
        'QT_TUR_INF',
        'QT_TUR_FUND',
        'QT_TUR_MED',
        'QT_TUR_ESP'
    ]

    df_fin_city_year = filter_financial_source(df_fin_city, year)

    teacher_spending = df_fin_city_year[
        (df_fin_city_year["eixo"] == "Pessoal") &
        (df_fin_city_year["macro"] == "Magistério/Docentes")
    ]

    teacher_spending_total = teacher_spending["valor"].sum()

    total_teachers_city = df_enroll_wide[teacher_cols].sum().sum()

    avg_salary_teacher = (
        teacher_spending_total / total_teachers_city
        if total_teachers_city > 0 else 0
    )

    salary_score = 1 - min(avg_salary_teacher / 6000, 1)

    ratings_map = {}

    for school_id in active_schools_ids:

        if school_id not in df_enroll_wide.index:
            continue

        school_data = df_enroll_wide.loc[school_id]

        teachers = sum(
            school_data.get(col, 0)
            for col in teacher_cols
        )

        classes = sum(
            school_data.get(col, 0)
            for col in class_cols
        )

        students = students_by_school.get(school_id, 0)

        if teachers == 0:
            ratings_map[school_id] = 0
            continue

        students_per_teacher = students / teachers
        classes_per_teacher = classes / teachers

        students_score = min(students_per_teacher / 40, 1)
        classes_score = min(classes_per_teacher / 3, 1)

        stress = 1 - round(
            0.4 * students_score +
            0.3 * classes_score +
            0.3 * salary_score,
            2
        )

        stress = max(0, min(round(stress, 2), 1))

        ratings_map[school_id] = stress

    return pd.Series(ratings_map)

def get_teacher_instability_rating(active_schools_ids, df_fin_city, students_by_school, year):

    df_year = filter_financial_source(df_fin_city, year)

    teacher_spending = df_year[
        (df_year["eixo"] == "Pessoal") &
        (df_year["macro"] == "Magistério/Docentes")
    ]

    temp_spending = teacher_spending[
        teacher_spending["micro"] == "Contrato Temporário"
    ]

    total_teacher = teacher_spending["valor"].sum()
    total_temp = temp_spending["valor"].sum()

    if total_teacher == 0:
        return pd.Series(index=active_schools_ids, data=0)

    temp_ratio = total_temp / total_teacher
    instability_score = 1 - min(temp_ratio / 0.4, 1)

    total_students_city = students_by_school.sum()

    ratings = {}

    for sid in active_schools_ids:

        students = students_by_school.get(sid, 0)

        share = students / total_students_city if total_students_city > 0 else 0

        score = instability_score * share

        ratings[sid] = max(0,min(score,1))

    return pd.Series(ratings)

def get_administrative_burden_rating(active_schools_ids, df_fin_city, students_by_school, year):

    df_year = filter_financial_source(df_fin_city, year)

    total_spending = df_year["valor"].sum()

    admin_spending = df_year[
        df_year["eixo"] == "Gestão e Administração"
    ]

    admin_total = admin_spending["valor"].sum()

    if total_spending == 0:
        return pd.Series(index=active_schools_ids, data=0)

    admin_ratio = admin_total / total_spending
    admin_score = 1 - min(admin_ratio / 0.15, 1)

    total_students_city = students_by_school.sum()

    ratings = {}

    for sid in active_schools_ids:

        students = students_by_school.get(sid, 0)

        share = students / total_students_city if total_students_city > 0 else 0

        score = admin_score * share * len(active_schools_ids)

        ratings[sid] = max(0, min(round(score,2),1))

    return pd.Series(ratings)

def get_spending_per_student(active_schools_ids, df_fin_city, students_by_school, df_enroll_wide, year):

    df_year = filter_financial_source(df_fin_city, year)

    total_spending = df_year["valor"].sum()

    weights = build_school_weight(df_enroll_wide, students_by_school)
    total_weight = weights.sum()

    ratings = {}

    for sid in active_schools_ids:

        weight_school = weights.get(sid, 0)

        share = weight_school / total_weight if total_weight > 0 else 0

        school_spending = total_spending * share

        students_school = students_by_school.get(sid, 0)

        ratings[sid] = round(
            school_spending / students_school, 2
        ) if students_school > 0 else 0

    return pd.Series(ratings)

def get_spending_per_teacher(df_enroll_wide, active_schools_ids, df_fin_city, students_by_school, year):

    teacher_cols = [
        'QT_DOC_INF',
        'QT_DOC_FUND',
        'QT_DOC_MED',
        'QT_DOC_ESP'
    ]

    df_year = filter_financial_source(df_fin_city, year)

    total_spending = df_year["valor"].sum()

    teachers_by_school = df_enroll_wide[teacher_cols].sum(axis=1)

    weights = build_school_weight(df_enroll_wide, students_by_school)
    total_weight = weights.sum()

    ratings = {}

    for sid in active_schools_ids:

        share = weights.get(sid, 0) / total_weight if total_weight > 0 else 0

        school_spending = total_spending * share

        teachers_school = teachers_by_school.get(sid, 0)

        ratings[sid] = round(
            school_spending / teachers_school, 2
        ) if teachers_school > 0 else 0

    return pd.Series(ratings)

def get_pedagogical_spending_per_student(active_schools_ids, df_fin_city, students_by_school, df_enroll_wide, year):

    df_year = filter_financial_source(df_fin_city, year)

    pedagogical = df_year[
        (df_year["eixo"] == "Recursos Pedagógicos") |
        (
            (df_year["eixo"] == "Infraestrutura Escolar") &
            (df_year["macro"] == "Tecnologia Educacional")
        )
    ]

    total_pedagogical = pedagogical["valor"].sum()

    weights = build_school_weight(df_enroll_wide, students_by_school)
    total_weight = weights.sum()

    ratings = {}

    for sid in active_schools_ids:

        share = weights.get(sid, 0) / total_weight if total_weight > 0 else 0

        school_spending = total_pedagogical * share

        students_school = students_by_school.get(sid, 0)

        ratings[sid] = round(
            school_spending / students_school, 2
        ) if students_school > 0 else 0

    return pd.Series(ratings)

def get_infrastructure_spending_per_student(active_schools_ids, df_fin_city, students_by_school, df_enroll_wide, year):

    df_year = filter_financial_source(df_fin_city, year)

    infra = df_year[
        (df_year["eixo"] == "Infraestrutura Escolar") &
        (df_year["macro"].isin([
            "Obras",
            "Manutenção",
            "Infraestrutura Física"
        ]))
    ]

    total_infra = infra["valor"].sum()

    weights = build_school_weight(df_enroll_wide, students_by_school)
    total_weight = weights.sum()

    ratings = {}

    for sid in active_schools_ids:

        share = weights.get(sid, 0) / total_weight if total_weight > 0 else 0

        school_spending = total_infra * share

        students_school = students_by_school.get(sid, 0)

        ratings[sid] = round(
            school_spending / students_school, 2
        ) if students_school > 0 else 0

    return pd.Series(ratings)

def create_rating_table(df_infra_long, df_enroll_long, df_school_info, df_dict, year, dir_atual):

    df_active = df_school_info[df_school_info['funcionamento'] == 1].copy()
    
    df_school_ratings = pd.DataFrame(index=df_active['id_escola'])
    df_school_ratings['ano'] = year

    # Carregamento.
    df_dict_infra = pd.read_csv(os.path.join(dir_atual, "..", "data/Infraestrutura/infrastructure_dict.csv"))
    df_dict_enroll = pd.read_csv(os.path.join(dir_atual, "..", "data/Matricula/enroll_dict.csv"))
    
    df_fin_city = pd.read_csv(os.path.join(dir_atual, "..", "data/CONSOLIDADO_GERAL_FINAL.csv"))

    df_fin_city["data"] = pd.to_datetime(df_fin_city["data"], dayfirst=True)
    df_fin_city["ano"] = df_fin_city["data"].dt.year

    map_infra_names = dict(zip(df_dict_infra['id_atributo'], df_dict_infra['variavel']))

    df_infra_wide = df_infra_long.pivot_table(index='id_escola',columns='id_atributo',values='valor',aggfunc='first')
    df_infra_wide.columns = df_infra_wide.columns.map(map_infra_names)
    df_infra_wide = df_infra_wide.reindex(df_school_ratings.index).fillna(0)

    map_enroll_names = dict(zip(df_dict_enroll['id_atributo'], df_dict_enroll['variavel']))
    df_enroll_wide = df_enroll_long.pivot_table(index='id_escola',columns='id_atributo',values='valor',aggfunc='first')
    df_enroll_wide.columns = df_enroll_wide.columns.map(map_enroll_names)
    df_enroll_wide = df_enroll_wide.reindex(df_school_ratings.index).fillna(0)

    # Criar maps rapidos para futuros calculos nos ratings
    students_by_school = build_students_map(df_enroll_wide)
    support_staff_by_school = build_support_staff_by_school_map(df_enroll_wide)


    df_school_ratings['acessibility_rating'] = get_acessible_rating(df_infra_wide, df_school_ratings.index, df_fin_city, students_by_school, year)
    df_school_ratings['recreation_rating'] = get_recreation_rating(df_infra_wide, df_school_ratings.index, df_fin_city, students_by_school, year)
    df_school_ratings['wellbeing_rating'] = get_wellbeing_rating(df_infra_wide, df_school_ratings.index, df_fin_city, students_by_school, year)
    df_school_ratings['human_support_rating'] = get_human_support_rating(df_enroll_wide, df_school_ratings.index, df_fin_city, students_by_school, support_staff_by_school, year)
    df_school_ratings['management_rating'] = get_management_rating(df_enroll_wide, df_school_ratings.index)
    df_school_ratings['age_grade_distortion_rating'] = get_age_grade_distortion(df_enroll_wide, df_school_ratings.index)
    df_school_ratings['pedagogical_rating'] = get_pedagogical_rating(df_infra_wide, df_school_ratings.index, df_fin_city, students_by_school, year) 
    df_school_ratings['teacher_stress_rating'] = get_teacher_stress_rating(df_enroll_wide, df_school_ratings.index, df_fin_city, students_by_school, year)
    df_school_ratings['teacher_instability_rating']   = get_teacher_instability_rating(df_school_ratings.index, df_fin_city, students_by_school, year)
    df_school_ratings['administrative_burden_rating'] = get_administrative_burden_rating(df_school_ratings.index, df_fin_city, students_by_school, year)
    df_school_ratings['spending_per_student'] = get_spending_per_student(df_school_ratings.index, df_fin_city, students_by_school, df_enroll_wide, year)
    df_school_ratings['spending_per_teacher'] = get_spending_per_teacher(df_enroll_wide, df_school_ratings.index, df_fin_city, students_by_school, year)
    df_school_ratings['pedagogical_spending_per_student'] = get_pedagogical_spending_per_student(df_school_ratings.index,df_fin_city,students_by_school, df_enroll_wide, year)
    df_school_ratings['infraestructure_speding_per_student'] = get_infrastructure_spending_per_student(df_school_ratings.index,df_fin_city,students_by_school, df_enroll_wide, year)


    path_ratings = os.path.join(dir_atual, "..", "data/Geral/school_ratings.csv")
    save_incremental(df_school_ratings.reset_index(), path_ratings)
    
    print(df_school_ratings.head(10))