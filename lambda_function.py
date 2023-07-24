import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
import json
import boto3
import os
from dateutil.parser import parse
import datetime
import numpy as np
import urllib3
import pandas as pd
import sqlalchemy as sd
import os

error_msg = list()
success_msg = list()

def lambda_handler(event, context):
    global error_msg
    global success_msg
    error_msg.clear()
    success_msg.clear()
    ssm = boto3.client("ssm")
    host_client = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user_name = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    user_password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")

    db_name = "seronetdb-Vaccine_Response"
    connection_tuple = connect_to_sql_db(host_client, user_name, user_password, db_name)
    update_participant_info(connection_tuple)
    make_time_line(connection_tuple)
    failure = ssm.get_parameter(Name="failure_hook_url", WithDecryption=True).get("Parameter").get("Value")
    success = ssm.get_parameter(Name="success_hook_url", WithDecryption=True).get("Parameter").get("Value")
    if len(error_msg) > 1:
        message_slack_fail = ''
        for error_message in error_msg:
            message_slack_fail = message_slack_fail + '\n'+ error_message
        write_to_slack(message_slack_fail, failure)
    else:
        message_slack_success = ''
        for success_message in success_msg:
            message_slack_success = message_slack_success + '\n'+ success_message
        write_to_slack(message_slack_success, success)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def connect_to_sql_db(host_client, user_name, user_password, file_dbname):
    global error_msg
    #host_client = "Host Client"
    #user_name = "User Name"
    #user_password = "User Password"
    #file_dbname = "Name of database"

    sql_column_df = pd.DataFrame(columns=["Table_Name", "Column_Name", "Var_Type", "Primary_Key", "Autoincrement",
                                          "Foreign_Key_Table", "Foreign_Key_Column"])
    creds = {'usr': user_name, 'pwd': user_password, 'hst': host_client, "prt": 3306, 'dbn': file_dbname}
    connstr = "mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}"
    engine = sd.create_engine(connstr.format(**creds))
    engine = engine.execution_options(autocommit=False)
    conn = engine.connect()
    metadata = sd.MetaData()
    metadata.reflect(engine)

    for t in metadata.tables:
        try:
            curr_table = metadata.tables[t]
            curr_table = curr_table.columns.values()
            for curr_row in range(len(curr_table)):
                curr_dict = {"Table_Name": t, "Column_Name": str(curr_table[curr_row].name),
                             "Var_Type": str(curr_table[curr_row].type),
                             "Primary_Key": str(curr_table[curr_row].primary_key),
                             "Autoincrement": False,
                             "Foreign_Key_Count": 0,
                             "Foreign_Key_Table": 'None',
                             "Foreign_Key_Column": 'None'}
                curr_dict["Foreign_Key_Count"] = len(curr_table[curr_row].foreign_keys)
                if curr_table[curr_row].autoincrement is True:
                    curr_dict["Autoincrement"] = True
                if len(curr_table[curr_row].foreign_keys) == 1:
                    key_relation = list(curr_table[curr_row].foreign_keys)[0].target_fullname
                    key_relation = key_relation.split(".")
                    curr_dict["Foreign_Key_Table"] = key_relation[0]
                    curr_dict["Foreign_Key_Column"] = key_relation[1]


                sql_column_df = pd.concat([sql_column_df, pd.DataFrame.from_records([curr_dict])])
        except Exception as e:
            error_msg.append(str(e))
            display_error_line(e)
    print("## Sucessfully Connected to " + file_dbname + " ##")
    sql_column_df.reset_index(inplace=True, drop=True)
    return sql_column_df, engine, conn


def update_participant_info(connection_tuple):
    conn = connection_tuple[2]
    engine = connection_tuple[1]
#########  Update the first visit offset correction table for new participants ###################
    offset_data = pd.read_sql(("SELECT * FROM `seronetdb-Vaccine_Response`.Visit_One_Offset_Correction;"), conn)
    visit_data = pd.read_sql(("SELECT Research_Participant_ID, Visit_Date_Duration_From_Index FROM `seronetdb-Vaccine_Response`.Participant_Visit_Info " +
                              "where Type_Of_Visit = 'Baseline' and Visit_Number = '1';"), conn)

    merged_data = visit_data.merge(offset_data, left_on=visit_data.columns.tolist(), right_on=offset_data.columns.tolist(), how="left", indicator=True)
    new_data = merged_data.query("_merge not in ['both']")
    new_data = new_data.drop(["Offset_Value", "_merge"], axis=1)
    new_data = new_data.rename(columns={"Visit_Date_Duration_From_Index":"Offset_Value"})

    check_data = new_data.merge(offset_data["Research_Participant_ID"], how="left", indicator=True)
    new_data = check_data.query("_merge not in ['both']").drop("_merge", axis=1)
    update_data = check_data.query("_merge in ['both']").drop("_merge", axis=1)
    try:
        new_data.to_sql(name="Visit_One_Offset_Correction", con=engine, if_exists="append", index=False)
    except Exception as e:
        display_error_line(e)
    finally:
        conn.connection.commit()
    for curr_part in update_data.index:
        try:
            sql_qry = (f"update Visit_One_Offset_Correction set Offset_Value = '{update_data.loc[curr_part, 'Offset_Value']}' " +
                       f"where Research_Participant_ID = '{update_data.loc[curr_part, 'Research_Participant_ID']}'")
            conn.execute(sql_qry)
        except Exception as e:
            display_error_line(e)
        finally:
            conn.connection.commit()

#########  Update the "Sunday_Prior_To_First_Visits" feild using accrual reports ###################
    accrual_data = pd.read_sql(("SELECT Research_Participant_ID, Sunday_Prior_To_Visit_1 FROM Accrual_Participant_Info;"), conn)
    part_data = pd.read_sql(("SELECT Research_Participant_ID, Sunday_Prior_To_First_Visit FROM Participant;"), conn)

    merged_data = part_data.merge(accrual_data, on="Research_Participant_ID", how="left", indicator=True)
    check_data = merged_data.query("_merge in ['both']")
    check_data = check_data.query("Sunday_Prior_To_First_Visit !=Sunday_Prior_To_Visit_1")
    for curr_part in check_data.index:
        try:
            sql_qry = (f"update Participant set Sunday_Prior_To_First_Visit = '{check_data.loc[curr_part, 'Sunday_Prior_To_Visit_1']}' " +
                       f"where Research_Participant_ID = '{check_data.loc[curr_part, 'Research_Participant_ID']}'")
            conn.execute(sql_qry)
        except Exception as e:
            display_error_line(e)
        finally:
            conn.connection.commit()
#########  Update the Primary Cohort feild using accrual reports ###################
    accrual_data = pd.read_sql(("SELECT Research_Participant_ID, Visit_Number, Site_Cohort_Name, Primary_Cohort FROM Accrual_Visit_Info;"), conn)
    visit_data = pd.read_sql(("SELECT Visit_Info_ID, Primary_Study_Cohort, CBC_Classification FROM Participant_Visit_Info;"), conn)
    accrual_data["New_Visit_Info_ID"] = (accrual_data["Research_Participant_ID"] + " : V" + ["%02d" % (int(i),) for i in accrual_data['Visit_Number']])
    x = visit_data["Visit_Info_ID"].str.replace(": B", ": V")
    visit_data["New_Visit_Info_ID"] = x.str.replace(": F", ": V")
    x = visit_data.merge(accrual_data, how="left", on= "New_Visit_Info_ID")
    x.fillna("No Data", inplace=True)
    y = x.query("Primary_Study_Cohort != Primary_Cohort or CBC_Classification != Site_Cohort_Name")
    y = y.query("Primary_Cohort not in ['No Data']")  #no accrual data to update
    for curr_part in y.index:
        try:
            sql_qry = (f"update Participant_Visit_Info set CBC_Classification = '{y.loc[curr_part, 'Site_Cohort_Name']}', " +
                       f"Primary_Study_Cohort = '{y.loc[curr_part, 'Primary_Cohort']}' " +
                       f"where Visit_Info_ID = '{y.loc[curr_part, 'Visit_Info_ID']}'")
            conn.execute(sql_qry)
        except Exception as e:
            display_error_line(e)
        finally:
            conn.connection.commit()

    ### sets version number of all visits ##
    version_num = pd.read_sql(("select * from  `seronetdb-Vaccine_Response`.Participant_Visit_Info as v " +
                              "join `seronetdb-Vaccine_Response`.Participant as p on v.Research_Participant_ID = p.Research_Participant_ID"), conn)

    version_num = version_num.query("Submission_Index == 76")
    for index in version_num.index:
        try:
            visit_id = version_num["Visit_Info_ID"][index]
            sql_query = f"Update Participant_Visit_Info set Data_Release_Version = '3.0.0' where Visit_Info_ID = '{visit_id }'"
            conn.execute(sql_query)
        except Exception as e:
            display_error_line(e)
        finally:
            conn.connection.commit()







def make_time_line(connection_tuple):
    global success_msg
    global error_msg
    pd.options.mode.chained_assignment = None  # default='warn'
    conn = connection_tuple[2]
    engine = connection_tuple[1]

    x = pd.read_sql(("SELECT Sunday_Prior_To_First_Visit FROM `seronetdb-Vaccine_Response`.Participant;"), conn)
    success_msg.append("## Updating vaccine response timeline ##")

    bio_data = pd.read_sql("Select Visit_Info_ID, Research_Participant_ID, Biospecimen_ID, Biospecimen_Type, " +
                       "Biospecimen_Collection_Date_Duration_From_Index, Biospecimen_Comments " +
                       "from `seronetdb-Vaccine_Response`.Biospecimen", conn)

    ali_data = pd.read_sql(("Select * from Aliquot as a where ((a.Aliquot_Comments not in ('Aliquot does not exist; mistakenly included in initial file') " +
                            "and a.Aliquot_Comments not in ('Aliquot was unable to be located.') " +
                            "and a.Aliquot_Comments not in ('Previously submitted in error')  " +
                            "and a.Aliquot_Comments not in ('Serum aliquots were not collected and cannot be shipped.')  " +
                            "and a.Aliquot_Comments not in ('Expiration date not recorded, lot number not recorded; Sample unavailable, used in local experiments')  " +
                            "and a.Aliquot_Comments not in ('Expiration date not recorded, lot number not recorded; No cells in aliquot; all cells from sample in single aliquot'))  " +
                            "or a.Aliquot_Comments is NULL)"), conn)

    rows_to_remove_comments = ['Previously submitted in error', 'Biospecimens were not collected.']
    bio_data = bio_data.loc[~bio_data['Biospecimen_Comments'].isin(rows_to_remove_comments)]

    x = bio_data.merge(ali_data, how = "outer", indicator=True)
    y = pd.crosstab(x["Visit_Info_ID"], x["Biospecimen_Type"], values="Aliquot_Volume", aggfunc="count").reset_index()
    y = y.merge(x[["Visit_Info_ID", "Research_Participant_ID", "Biospecimen_Collection_Date_Duration_From_Index"]])


    accrual_visit = pd.read_sql(("SELECT av.Primary_Cohort, av.Research_Participant_ID, av.Visit_Number as 'Accrual_Visit_Num', ap.Sunday_Prior_To_Visit_1, " + 
                                 "av.Visit_Date_Duration_From_Visit_1 FROM `seronetdb-Vaccine_Response`.Accrual_Visit_Info as av " +
                                 "join `seronetdb-Vaccine_Response`.Accrual_Participant_Info as ap on av.Research_Participant_ID = ap.Research_Participant_ID"), conn)

    accrual_vacc = pd.read_sql(("SELECT * FROM `seronetdb-Vaccine_Response`.Accrual_Vaccination_Status "), conn) #+
                             #"Where Vaccination_Status not in ('Unvaccinated', 'No vaccination event reported')"), conn)
    accrual_vacc.drop("Visit_Number", axis=1, inplace=True)

    prev_visit_done = pd.read_sql(("SELECT * FROM Normalized_Visit_Vaccination;"), conn)
    prev_vacc_done = pd.read_sql(("SELECT * FROM Sample_Collection_Table;"), conn)

    all_vacc = pd.DataFrame(columns = ['Research_Participant_ID', 'Primary_Cohort', 'Normalized_Visit_Index', 'Duration_From_Visit_1', 'Vaccination_Status',
                              'SARS-CoV-2_Vaccine_Type', 'Duration_Between_Vaccine_and_Visit', 'Data_Status'])
    all_sample = pd.DataFrame(columns = ['Research_Participant_ID', 'Normalized_Visit_Index', 'Serum_Volume_For_FNL', 'Submitted_Serum_Volume',
                               'Serum_Volume_Received', 'Num_PBMC_Vials_For_FNL', 'Submitted_PBMC_Vials', 'PBMC_Vials_Received'])

    submit_visit = pd.read_sql(("SELECT v.Visit_Info_ID, v.Research_Participant_ID, v.Visit_Number as 'Submitted_Visit_Num', Primary_Study_Cohort, " + 
                               "v.Visit_Date_Duration_From_Index - o.Offset_Value as 'Duration_From_Baseline', p.Sunday_Prior_To_First_Visit " +
                               "FROM `seronetdb-Vaccine_Response`.Participant_Visit_Info as v join Visit_One_Offset_Correction as o " +
                               "on v.Research_Participant_ID = o.Research_Participant_ID " + 
                               "join `seronetdb-Vaccine_Response`.Participant as p on v.Research_Participant_ID = p.Research_Participant_ID"), conn)

    submit_vacc = pd.read_sql(("SELECT c.Research_Participant_ID, c.`SARS-CoV-2_Vaccine_Type`, c.Vaccination_Status, " +
                               "c.`SARS-CoV-2_Vaccination_Date_Duration_From_Index` - o.Offset_Value as 'SARS-CoV-2_Vaccination_Date_Duration_From_Index' " +
                               "FROM Covid_Vaccination_Status as c join Visit_One_Offset_Correction as o " +
                               "on c.Research_Participant_ID = o.Research_Participant_ID where " +
                               "(Covid_Vaccination_Status_Comments not in ('Dose was previously linked to the wrong visit number.') and " +
                                "Covid_Vaccination_Status_Comments not in ('Vaccine Status does not exist, input error') and " +
                                "Covid_Vaccination_Status_Comments not in ('Record previously submitted in error.')) " +
                                "or Covid_Vaccination_Status_Comments is NULL;"), conn)
    submit_vacc = submit_vacc.query("Vaccination_Status not in ['No vaccination event reported']")

    pending_samples = pd.read_sql(("SELECT Research_Participant_ID, Visit_Number as 'Accrual_Visit_Num', Serum_Volume_For_FNL, Num_PBMC_Vials_For_FNL FROM Accrual_Visit_Info"), conn)
    pending_samples.replace("N/A", np.nan, inplace=True)
    pending_samples["Serum_Volume_For_FNL"] = [float(i) for i in pending_samples["Serum_Volume_For_FNL"]]
    pending_samples["Num_PBMC_Vials_For_FNL"] = [float(i) for i in pending_samples["Num_PBMC_Vials_For_FNL"]]

    #data_samples = pd.read_sql(("SELECT  b.Visit_Info_ID, b.Research_Participant_ID, " +
    #                            "round(sum(case when b.Biospecimen_Type = 'Serum' then a.Aliquot_Volume else 0 end), 1) as 'Submitted_Serum_Volumne', " +
    #                            "sum(case when b.Biospecimen_Type = 'PBMC' then 1 else 0 end) as 'Submitted_PBMC_Vials' FROM Biospecimen as b " +
    #                            "join Aliquot as a on b.Biospecimen_ID = a.Biospecimen_ID group by b.Visit_Info_ID"), conn)

    data_samples = pd.read_sql(("Select v.Visit_Info_ID, round(sum(case when (ali.Aliquot_Volume is NULL) then 0 " +
                                                                       "when (b.Biospecimen_Type = 'Serum') and (ali.Aliquot_Volume <  bp.volume) then bp.Volume " +
                                                                       "when (b.Biospecimen_Type = 'Serum') and bp.volume is NULL then ali.Aliquot_Volume " +
                                                                       "when (b.Biospecimen_Type = 'Serum') and (ali.Aliquot_Volume >= bp.volume) then ali.Aliquot_Volume "+
                                                                       "when (bp.`Material Type` = 'Serum') and (b.Biospecimen_Type is NULL) then bp.Volume " +
                                                                       "else 0 end), 1)  as 'Submitted_Serum_Volume', " +
                               "round(sum(case when bp.`Material Type` is NULL then 0  when bp.`Material Type` = 'SERUM' and `Vial Status` not in ('Empty') then bp.Volume " +
                                              "when bp.`Material Type` = 'SERUM' and `Vial Status` in ('Empty')  then ali.Aliquot_Volume else 0 end), 1) as 'Serum_Volume_Received', " +

                              "sum(case when b.Biospecimen_Type = 'Serum' and  ali.Aliquot_Comments in ('Aliquot does not exist; mistakenly included in initial file', " +
                              "'Aliquot was unable to be located.', 'Previously submitted in error', 'Serum aliquots were not collected and cannot be shipped.', " +
                              "'Expiration date not recorded, lot number not recorded; Sample unavailable, used in local experiments', " +
                              "'Expiration date not recorded, lot number not recorded; No cells in aliquot; all cells from sample in single aliquot') then ali.Aliquot_Volume " +
                              "else 0 end) as 'Serum_Vial_Error', "
                              "sum(case when b.Biospecimen_Type = 'PBMC' then 1 else 0 end) as 'Submitted_PBMC_Vials', " +
                              "sum(case when bp.`Material Type` is NULL  then 0  " +
                                      "when bp.`Material Type` = 'PBMC' then 1 else 0 end) as 'PBMC_Vials_Received', " +
                              "sum(case when b.Biospecimen_Type = 'PBMC' and  ali.Aliquot_Comments in ('Aliquot does not exist; mistakenly included in initial file', " +
                              "'Aliquot was unable to be located.', 'Previously submitted in error', 'Serum aliquots were not collected and cannot be shipped.', " +
                              "'Expiration date not recorded, lot number not recorded; Sample unavailable, used in local experiments', " +
                              "'Expiration date not recorded, lot number not recorded; No cells in aliquot; all cells from sample in single aliquot') then ali.Aliquot_Volume " +
                              "else 0 end) as 'PBMC_Vial_Error' "

                              "from Participant_Visit_Info as v left join Biospecimen as b on v.Visit_Info_ID = b.Visit_Info_ID " +
                              "left join Aliquot as ali " +
                              "on b.Biospecimen_ID = ali.Biospecimen_ID   " +
                              "left join BSI_Parent_Aliquots as bp on ali.Aliquot_ID = bp.`Current Label` group by v.Visit_Info_ID"), conn)


    submit_visit = submit_visit.merge(data_samples, how="left")

    covid_hist = pd.read_sql(("SELECT ch.Visit_Info_ID, ch.COVID_Status, ch.Breakthrough_COVID, ch.Average_Duration_Of_Test, o.Offset_Value " +
                              "FROM `seronetdb-Vaccine_Response`.Covid_History as ch " +
                              "join `seronetdb-Vaccine_Response`.Visit_One_Offset_Correction as o " +
                              "on o.Research_Participant_ID = left(ch.Visit_Info_ID,9)"), conn)

    uni_part = list(set(accrual_visit["Research_Participant_ID"].tolist() + submit_visit["Research_Participant_ID"].tolist()))
    #uni_part = ["32_441084"]
    error_list = []
    error_uni = []
    for curr_part in uni_part:
        #if curr_part == '41_100001':
        #    print("x")

        curr_visit = submit_visit.query("Research_Participant_ID == @curr_part")
        curr_vacc = submit_vacc.query("Research_Participant_ID == @curr_part")
        acc_visit = accrual_visit.query("Research_Participant_ID == @curr_part")
        acc_vacc = accrual_vacc.query("Research_Participant_ID == @curr_part")

        curr_visit["Submitted_Serum_Volume"] = curr_visit["Submitted_Serum_Volume"] - curr_visit["Serum_Vial_Error"]
        curr_visit['Submitted_PBMC_Vials'] =  curr_visit['Submitted_PBMC_Vials'] - curr_visit["PBMC_Vial_Error"]
        curr_visit.drop(["Serum_Vial_Error", "PBMC_Vial_Error"], inplace=True, axis=1)

        curr_visit.rename(columns={"Duration_From_Baseline": "Duration_From_Visit_1"}, inplace=True)
        curr_vacc.rename(columns={"SARS-CoV-2_Vaccination_Date_Duration_From_Index": "Duration_From_Visit_1"}, inplace=True)
        acc_visit.rename(columns={"Visit_Date_Duration_From_Visit_1": "Duration_From_Visit_1"}, inplace=True)
        acc_vacc.rename(columns={"SARS-CoV-2_Vaccination_Date_Duration_From_Visit1": "Duration_From_Visit_1"}, inplace=True)

        y = covid_hist[covid_hist["Visit_Info_ID"].apply(lambda x: x[:9] == curr_part)]
        y["Breakthrough_COVID"].fillna("No Covid Event Reported", inplace=True)
        y["Average_Duration_Of_Test"].fillna("NAN", inplace=True)
        y = y.replace("NAN", np.nan)
        y = y.query("Average_Duration_Of_Test == Average_Duration_Of_Test")
        try:
            y["Average_Duration_Of_Test"] = [float(i) for i in y["Average_Duration_Of_Test"]]
        except Exception as e:
            error_msg.append(str(e))
            display_error_line(e)

        y = y.query("Average_Duration_Of_Test >= 0 or Average_Duration_Of_Test <= 0")

        if len(curr_vacc) > 0:
            curr_vacc = curr_vacc.merge(acc_vacc, on=["Research_Participant_ID", "Vaccination_Status", "SARS-CoV-2_Vaccine_Type"], how="outer")
            if "Duration_From_Visit_1_x" in curr_vacc:
                acc_only = curr_vacc.query("Duration_From_Visit_1_x != Duration_From_Visit_1_x")
                curr_vacc["Duration_From_Visit_1_x"][acc_only.index] = curr_vacc["Duration_From_Visit_1_y"][acc_only.index]
                curr_vacc.drop("Duration_From_Visit_1_y", inplace=True, axis=1)
                curr_vacc.columns = [i.replace("_x", "") for i in curr_vacc.columns]

        try:
            curr_vacc["Duration_From_Visit_1"] = curr_vacc["Duration_From_Visit_1"].replace('N/A', -1000)
            curr_vacc["Duration_From_Visit_1"] = [int(i) if i == i else i for i in curr_vacc["Duration_From_Visit_1"]]
        except Exception as e:
            print(e)

        if len(curr_visit) > 0:
            curr_visit = clean_up_visit(curr_visit, curr_vacc)
        else:
            curr_visit["Duration_Between_Vaccine_and_Visit"] = 0
            curr_visit['Normalized_Visit_Index'] = 0
            curr_visit['Visit_Sample_Index'] = np.nan

        curr_visit, error_uni = combine_visit_and_vacc(curr_visit, error_uni, curr_part)

        curr_visit["Covid_Test_Result"] = "No Test Reported"
        curr_visit["Test_Duration_Since_Vaccine"] = "N/A"
        curr_visit["Previous Vaccion Dosage"] = "N/A"

        if len(y) > 0:
            curr_visit = add_covid_test(curr_visit, curr_vacc, y)
        acc_visit = clean_up_visit(acc_visit, acc_vacc)
        acc_visit, error_uni = combine_visit_and_vacc(acc_visit, error_uni, curr_part)


        try:
            #x = acc_visit.merge(curr_visit, on=["Research_Participant_ID", "Duration_Between_Vaccine_and_Visit", "Normalized_Visit_Index"], how="outer")
            x = acc_visit.merge(curr_visit, on=["Research_Participant_ID", "Normalized_Visit_Index"], how="outer")

            #x2 = x[(x["Duration_From_Visit_1_x"] - x["Duration_From_Visit_1_y"]) > 6]
            #if len(x2) > 0:
            #    print("x")

            x = x.sort_values(['Normalized_Visit_Index', 'Duration_From_Visit_1_y'])
            x = x.drop_duplicates('Normalized_Visit_Index', keep='first').reset_index(drop=True)
        except Exception as e:
            error_msg.append(str(e))
            display_error_line(e)

        try:
            x["Data_Status"] = "Accrual Data: Future"
            sub_data = x.query("Submitted_Visit_Num == Submitted_Visit_Num")
            x["Data_Status"][sub_data.index] = "Submitted_Data: Current"
            if "SARS-CoV-2_Vaccine_Type_y" in sub_data:
                x["SARS-CoV-2_Vaccine_Type_x"][sub_data.index] = sub_data["SARS-CoV-2_Vaccine_Type_y"]
            if "Vaccination_Status_y" in sub_data:
                x["Vaccination_Status_x"][sub_data.index] = sub_data["Vaccination_Status_y"]
            if "Duration_Between_Vaccine_and_Visit_y" in sub_data:
                x["Duration_Between_Vaccine_and_Visit_x"][sub_data.index] = sub_data["Duration_Between_Vaccine_and_Visit_y"]
            #if "Primary_Study_Cohort" in sub_data:
            #    x["Primary_Cohort"][sub_data.index] = sub_data["Primary_Study_Cohort"]
            if "Duration_From_Visit_1_x" in sub_data:
                x["Duration_From_Visit_1_x"][sub_data.index] = sub_data["Duration_From_Visit_1_y"]
        except Exception as e:
            error_msg.append(str(e))
            display_error_line(e)

        try:
            x = x.merge(pending_samples, how="left")
            #x["Serum_Volume_For_FNL"][sub_data.index] = sub_data["Submitted_Serum_Volumne"]
            #x["Num_PBMC_Vials_For_FNL"][sub_data.index] = sub_data["Submitted_PBMC_Vials"]
            x.columns = [i.replace("_x", "") for i in x.columns]
            try:
                start_date = max(d for d in x["Sunday_Prior_To_Visit_1"] if isinstance(d, datetime.date))
            except Exception:   #sunday prior is missing from accrual
                try:
                    start_date = max(d for d in x['Sunday_Prior_To_First_Visit'] if isinstance(d, datetime.date))
                except Exception:
                    start_date = datetime.date(2000,1,1)
                    error_list.append(curr_part)
            x["Sunday_Prior_To_Visit_1"].fillna(start_date, inplace=True)
            x['Date_Of_Event'] = x["Sunday_Prior_To_Visit_1"] + [datetime.timedelta(days = i) for i in x["Duration_From_Visit_1"]]

            try:
                x["Primary_Cohort"] = [i.split("|")[0] for i in x["Primary_Cohort"]]
            except Exception:
                error = x.query("Primary_Cohort != Primary_Cohort")
                x.loc[error.index,"Primary_Cohort"] = error["Primary_Study_Cohort"]
                #print(error["Visit_Info_ID"])
                #x = x.query("Primary_Cohort == Primary_Cohort")

            x["Visit_Info_ID"].fillna("N/A", inplace=True)
            col_list_1 = ['Research_Participant_ID', 'Primary_Cohort', 'Normalized_Visit_Index', 'Accrual_Visit_Num', 'Visit_Info_ID', 'Duration_From_Visit_1', 'Vaccination_Status',
                        'SARS-CoV-2_Vaccine_Type', 'Duration_Between_Vaccine_and_Visit', 'Data_Status']
            col_list_2 = ['Research_Participant_ID', 'Normalized_Visit_Index', 'Date_Of_Event', 'Serum_Volume_For_FNL', 'Submitted_Serum_Volume', "Serum_Volume_Received",
                        'Num_PBMC_Vials_For_FNL', 'Submitted_PBMC_Vials', "PBMC_Vials_Received"]
            all_vacc = pd.concat([all_vacc, x[col_list_1]])
            all_sample = pd.concat([all_sample, x[col_list_2]])

        except Exception as e:
            display_error_line(e) #stop the whole function when there is nothing to add to the table
            #error_list.append(curr_part)

    error_list = list(set(error_list))
    print("The research participant ids that both Sunday_Prior_To_First_Visit in the Participant table is null and Sunday_Prior_To_Visit_1 in the Accrual_Participant_Info table is null at the same time: ",error_list)
    print("The research participant ids that do not have data in table Accrual_visit_info:",error_uni)

    try:
        all_vacc.reset_index(inplace=True, drop=True)
        all_vacc.drop_duplicates(inplace=True)
        all_sample.replace(np.nan, -1, inplace=True)

        all_sample.reset_index(inplace=True, drop=True)
        z = all_sample.query("Serum_Volume_Received > Submitted_Serum_Volume")
        all_sample.loc[z.index, "Serum_Volume_Received"] = z["Submitted_Serum_Volume"]

        z = all_sample.query("Submitted_Serum_Volume > Serum_Volume_For_FNL")
        all_sample.loc[z.index, "Serum_Volume_For_FNL"] = z["Submitted_Serum_Volume"]

        z = all_sample.query("PBMC_Vials_Received > Submitted_PBMC_Vials")
        all_sample.loc[z.index, "Submitted_PBMC_Vials"] = z["PBMC_Vials_Received"]

        z = all_sample.query("Submitted_PBMC_Vials > Num_PBMC_Vials_For_FNL")
        all_sample.loc[z.index, "Num_PBMC_Vials_For_FNL"] = z["Submitted_PBMC_Vials"]



        #all_sample["Serum_Volume_Received"][z.index] = all_sample["Submitted_Serum_Volume"][z.index]
        #all_vacc["Breakthrough_To_Visit_Duration"] = all_vacc["Duration_From_Visit_1"] - all_vacc["Breakthrough_To_Visit_Duration"]
        #x = all_vacc.query("Breakthrough_Prior_To_Visit not in ['Yes']")
        #all_vacc["Breakthrough_To_Visit_Duration"][x.index] = np.nan

        primary_key = ["Research_Participant_ID", "Normalized_Visit_Index"]
        #have_samples = all_sample.query("Serum_Volume_For_FNL > 0 or Num_PBMC_Vials_For_FNL > 0")

        #all_vacc = all_vacc.query("Primary_Cohort == Primary_Cohort")
        #all_vacc["Primary_Cohort"] = [i.split("|")[0] for i in all_vacc["Primary_Cohort"]]

        #add_data_to_tables(all_vacc.loc[have_samples.index], prev_visit_done, primary_key, "Normalized_Visit_Vaccination", conn, engine)
        #add_data_to_tables(all_sample.loc[have_samples.index],prev_vacc_done, primary_key, "Sample_Collection_Table", conn, engine)
        add_data_to_tables(all_vacc, prev_visit_done, primary_key, "Normalized_Visit_Vaccination", conn, engine)
        add_data_to_tables(all_sample,prev_vacc_done, primary_key, "Sample_Collection_Table", conn, engine)

    except Exception as e:
        error_msg.append(str(e))
        display_error_line(e)
    print('file done')

def clean_up_visit(visit, vaccine):
    try:
        visit = visit.sort_values(["Duration_From_Visit_1"],ascending=[True])
        visit["Normalized_Visit_Index"] = list(range(1, len(visit)+1))
        df = pd.concat([visit, vaccine])
        df["Duration_From_Visit_1"].replace("Not Reported", -1000, inplace=True)
        df["Duration_From_Visit_1"].replace("Not reported", -1000, inplace=True)
        df["Duration_From_Visit_1"].replace("N/A", -1000, inplace=True)
        df["Duration_From_Visit_1"].fillna(-1000, inplace=True)
        df["Duration_From_Visit_1"] = [-1000 if i != i else float(i) for i in df["Duration_From_Visit_1"]]
        df["Vaccination_Status"] = df["Vaccination_Status"].replace(np.nan, "N/A")
        df = df.sort_values(["Duration_From_Visit_1", "Vaccination_Status"],ascending=[True, False])
        df["Duration_Between_Vaccine_and_Visit"] = 0
        df.reset_index(inplace=True, drop=True)
    except Exception as e:
        display_error_line(e)
    return df


def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,
                      "name": tb.tb_frame.f_code.co_name,
                      "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))


def combine_visit_and_vacc(df, error_uni, current_uni):
    if len(df) == 0:
        return df, error_uni
    try:
        last_visit = int(np.nanmax(df["Normalized_Visit_Index"]))
        for visit_num in list(range(1, last_visit+1)):
            visit_index = df.query(f"Normalized_Visit_Index == {visit_num}") #index of the visit
            if len(visit_index) == 0:
                print("visit does not exist")
                continue
            df = get_vaccine_data(df, visit_index, last_visit)
        df = df.query("Normalized_Visit_Index > 0")
    except Exception as e:
        error_uni.append(current_uni)
        print(current_uni)
        #print(e)
    return df, error_uni


def add_covid_test(df, curr_vacc, y):
    for indx in y.index:
        duration = y['Average_Duration_Of_Test'][indx] - y['Offset_Value'][indx]        #time of test relative to visit 1
        z = df.query("Duration_From_Visit_1 > @duration")
        if len(z) > 0:
            visit_index = z.index[0]
        else:
            visit_index = df[-1:].index.tolist()[0]
        if y["COVID_Status"][indx].find("Likely") >= 0:
            df["Covid_Test_Result"][visit_index] = "Likely Postivie"
        if y["COVID_Status"][indx].find("Positive") >= 0:
            df["Covid_Test_Result"][visit_index] = "Postivie Test"
        elif y["COVID_Status"][indx].find("Negative") >= 0:
            df["Covid_Test_Result"][visit_index] = "Negative Test"
        df["Test_Duration_Since_Vaccine"][visit_index] = duration
        try:
            x = curr_vacc.query("Duration_From_Visit_1 <= @duration")
            x.reset_index(inplace=True, drop=True)
            if len(x) == len(curr_vacc):     # infection occured after last vaccine
                df["Previous Vaccion Dosage"][visit_index] = curr_vacc[-1:]["Vaccination_Status"]
                df["Test_Duration_Since_Vaccine"][visit_index] = df["Test_Duration_Since_Vaccine"][visit_index] - curr_vacc[-1:]["Duration_From_Visit_1"]
            elif len(x) == 0:
                df["Previous Vaccion Dosage"][visit_index] = "Unvaccinated"
            else:
                df["Previous Vaccion Dosage"][visit_index] = df["Vaccination_Status"][visit_index]
                df["Test_Duration_Since_Vaccine"][visit_index] = duration - x[-1:]["Duration_From_Visit_1"].iloc[0]
        except Exception as e:
            display_error_line(e)
    return df


def get_vaccine_data(curr_sample, visit_index, last_visit):
    for offset in list(range(1, last_visit+1)):
        try:
            test_val = visit_index.index.tolist()[0]-offset
            if test_val >= 0:
                if curr_sample["Normalized_Visit_Index"][test_val] > 0:
                    continue
                elif curr_sample.loc[test_val]["Vaccination_Status"] ==  'No vaccination event reported':
                    continue 
                else:
                    curr_sample["Vaccination_Status"][test_val+offset] = curr_sample.loc[test_val]["Vaccination_Status"]
                    if curr_sample.loc[test_val]["Vaccination_Status"] == "Unvaccinated":
                        curr_sample["SARS-CoV-2_Vaccine_Type"][test_val+offset] = "N/A"
                        curr_sample["Duration_Between_Vaccine_and_Visit"][test_val+offset]= np.nan
                    else:
                        curr_sample["SARS-CoV-2_Vaccine_Type"][test_val+offset] = curr_sample.loc[test_val]["SARS-CoV-2_Vaccine_Type"]
                        duration = curr_sample.loc[test_val + offset]["Duration_From_Visit_1"] - curr_sample.loc[test_val]["Duration_From_Visit_1"]
                        curr_sample["Duration_Between_Vaccine_and_Visit"][test_val+offset]= duration
                    break
            else:
                curr_sample["Vaccination_Status"][test_val+offset] = "No Vaccination Data"
                curr_sample["SARS-CoV-2_Vaccine_Type"][test_val+offset] = "N/A"
                curr_sample["Duration_Between_Vaccine_and_Visit"][test_val+offset]=  np.nan
        except Exception as e:
            print(curr_sample['Research_Participant_ID'])
            display_error_line(e)
    return curr_sample


def add_data_to_tables(df, prev_df, primary_key, table_name, conn, engine):
    global success_msg
    #if "Breakthrough_To_Visit_Duration" in df.columns:
    #    df["Breakthrough_To_Visit_Duration"].fillna(-10000, inplace=True)
    #    df['Duration_Between_Vaccine_and_Visit'].fillna(-10000, inplace=True)
    df.fillna("-10000", inplace=True)
    #if "Breakthrough_To_Visit_Duration" in prev_df.columns:
    #    prev_df["Breakthrough_To_Visit_Duration"].fillna(-10000, inplace=True)
    #    prev_df['Duration_Between_Vaccine_and_Visit'].fillna(-10000, inplace=True)
    prev_df.fillna("-10000", inplace=True)
    if "Serum_Volume_For_FNL" in df.columns:
        for curr_col in df.columns:
            if curr_col in ["Research_Participant_ID", 'Date_Of_Event']:
                pass
            else:
                df[curr_col] = [float(i) for i in df[curr_col]]

    #prev_df.replace(-10000, np.nan, inplace=True)
    #prev_df.replace("-10000", np.nan, inplace=True)
    if "Duration_Between_Vaccine_and_Visit" in prev_df.columns:
        prev_df["Duration_Between_Vaccine_and_Visit"] = [int(i) for i in prev_df["Duration_Between_Vaccine_and_Visit"]]
    if "Duration_Between_Vaccine_and_Visit" in df.columns:
        df["Duration_Between_Vaccine_and_Visit"] = [int(i) for i in df["Duration_Between_Vaccine_and_Visit"]]
    if "Duration_From_Visit_1" in prev_df.columns:
        prev_df["Duration_From_Visit_1"] = [int(i) for i in prev_df["Duration_From_Visit_1"]]

    if table_name == "Sample_Collection_Table":
        for curr_col in prev_df.columns:
            if curr_col in ["Research_Participant_ID", 'Date_Of_Event']:
                pass
            else:
                prev_df[curr_col] = [int(i) for i in prev_df[curr_col]]

    if "Accrual_Visit_Num" in df.columns:
        df["Accrual_Visit_Num"] = [int(i) for i in df["Accrual_Visit_Num"]]

    if "Accrual_Visit_Num" in prev_df.columns:
        prev_df["Accrual_Visit_Num"] = [int(i) for i in prev_df["Accrual_Visit_Num"]]


    try:
        merge_data = df.merge(prev_df, how="left", indicator=True)
        merge_data = merge_data.query("_merge not in ['both']").drop("_merge", axis=1)
        merge_data = merge_data.merge(prev_df, on=primary_key, how="left", indicator=True)
        new_data = merge_data.query("_merge == 'left_only'").drop('_merge', axis=1)
    except Exception as e:
        display_error_line(e)
    try:
        new_data.columns = [i.replace("_x", "") for i in new_data.columns]
        new_data = new_data[prev_df.columns]
        new_data = new_data.replace(-10000, 0)
        new_data.drop_duplicates(inplace=True)

        new_data.to_sql(name=table_name, con=engine, if_exists="append", index=False)
        conn.connection.commit()
        print(f"{len(new_data)} Visits have been added to {table_name}")
        success_msg.append(f"{len(new_data)} Visits have been added to {table_name}")
    except Exception as e:
       display_error_line(e)

    try:
        update_data = merge_data.query("_merge == 'both'").drop('_merge', axis=1)
        update_data.columns = [i.replace("_x", "") for i in update_data.columns]
        update_data = update_data[prev_df.columns]
        update_tables(conn, engine, primary_key, update_data, table_name)
        #print(f"{len(update_data)} Visits have been updated in {table_name}")
    except Exception as e:
        display_error_line(e)

def delete_data_files(bucket_name, file_key):
    global success_msg
    s3_resource = boto3.resource('s3') 
    bucket = s3_resource.Bucket(bucket_name)
    if 'Vaccine+Response+Submissions' in file_key or 'Reference+Panel+Submissions' in file_key:
        subfolders = file_key.split('/')
        # Get the first three sub folders
        new_file_key = os.path.join(subfolders[0], subfolders[1], subfolders[2])
        new_file_key = new_file_key.replace('+', ' ')
        for obj in bucket.objects.filter(Prefix = new_file_key):
            s3_resource.Object(bucket.name, obj.key).delete()
        print(f'{new_file_key} deleted')
        success_msg.append(f'{new_file_key} deleted')
    else:
        for obj in bucket.objects.filter(Prefix = file_key):
            s3_resource.Object(bucket.name, obj.key).delete()
        print(f'{file_key} deleted')
        success_msg.append(f'{file_key} deleted')

def update_tables(conn, engine, primary_keys, update_table, sql_table):
    global error_msg
    global success_msg
    key_str = ['`' + str(s) + '`' + " like '%s'" for s in primary_keys]
    key_str = " and ".join(key_str)

    col_list = update_table.columns.tolist()
    col_list = [i for i in col_list if i not in primary_keys]

    print(f"{len(update_table)} Visits have been updated in {sql_table}")
    success_msg.append(f"{len(update_table)} Visits have been updated in {sql_table}")
    for index in update_table.index:
        try:
            curr_data = update_table.loc[index, col_list].values.tolist()
            curr_data = [str(i).replace("'", "") for i in curr_data]
            curr_data = [i.replace('', "NULL") if len(i) == 0 else i for i in curr_data]

            primary_value = update_table.loc[index, primary_keys].values.tolist()
            primary_value = [str(i).replace(".0", "") for i in primary_value]
            update_str = ["`" + i + "` = '" + str(j) + "'" for i, j in zip(col_list, curr_data)]
            update_str = ', '.join(update_str)

            sql_query = (f"UPDATE {sql_table} set {update_str} where {key_str %tuple(primary_value)}")
            sql_query = sql_query.replace("'-10000'", "NULL")
            sql_query = sql_query.replace("'-10000.0'", "NULL")
            sql_query = sql_query.replace("like 'nan'", "is NULL")
            conn.execute(sql_query)
        except Exception as e:
            error_msg.append(str(e))
            display_error_line(e)
        finally:
            conn.connection.commit()

def write_to_slack(message_slack, slack_chanel):
    http = urllib3.PoolManager()
    data={"text": message_slack}
    r=http.request("POST", slack_chanel, body=json.dumps(data), headers={"Content-Type":"application/json"})
