# imports
import base64
import io
import json
import traceback
from datetime import datetime

import boto3
import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *

import redshift_utils as ru
import s3_utils as su

format_length = 150
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

secrets_client = boto3.client('secretsmanager')


# function - to get current date and time for logging
def current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M')


def get_elapsed_time(start_time, end_time):
    diff = end_time - start_time
    days, seconds = diff.days, diff.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    elapsed_time = f"{hours}:{minutes}:{seconds}"
    return elapsed_time


def equal_to_zero(res):
    res = res.split("|")
    tes_rest = []
    rep = ""
    for i in res:
        if int(float(i)) == 0:
            tes_rest.append("Pass")
        else:
            tes_rest.append("Fail")
        rep = str('|'.join([str(i) for i in tes_rest]))
    return rep


def equal_to_one(res):
    res = res.split("|")

    tes_rest = []
    rep = ""
    for i in res:
        if int(i) == 1:
            tes_rest.append("Pass")
        else:
            tes_rest.append("Fail")
        rep = str('|'.join([str(i) for i in tes_rest]))
    return rep


def compare_with_expected_value(stg, comp):
    stg = stg.split("|")
    comp = comp.split("|")
    tes_rest = []
    rep = ""
    for i in range(0, len(stg)):
        if stg[i] == 'ignore':
            tes_rest.append("Pass")
        elif stg[i] == 'any':
            tes_rest.append("Pass")
        elif stg[i] == comp[i]:
            tes_rest.append("Pass")
        else:
            tes_rest.append("Fail")
        rep = str('|'.join([str(t) for t in tes_rest]))
    return rep


def greater_than_one(res):
    res = res.split("|")
    tes_rest = []
    rep = ""
    for i in res:
        if int(i) > 0:
            tes_rest.append("Pass")
        else:
            tes_rest.append("Fail")
        rep = str('|'.join([str(i) for i in tes_rest]))
    return rep


def testcase_result(res):
    res = res.split("|")
    for ln in res:
        if ln == "Fail":
            return "Fail"
    return "Pass"


def report_parse(result):
    rn = result.split("|")
    fail_count = 0
    pass_count = 0

    html_report = []
    for gt in rn:
        if gt == 'Fail':
            fail_count = fail_count + 1
            html_report.append("<td bgcolor = '#ffb3b3'>" + gt + "</td>")
        else:
            pass_count = pass_count + 1
            html_report.append("<td bgcolor = '#b3ffec'>" + gt + "</td>")

    if fail_count > 0:
        test_report = ("<tr>" + " ".join(html_report)
                       + "</tr>" + "<tr><td bgcolor = '#b3ffff'> Testcase Summary </td><td bgcolor = '#00cc44'>"
                       + str(pass_count) + " : Attributes Passed " + "</td><td bgcolor = '#ff6666'>" +
                       str(fail_count) + " : Attributes Failed" + "</td></tr><tr><td bgcolor = '#b3ffff'>"
                       + "TestCase Status " + "</td><td bgcolor = '#ff1a1a'>" + "Failed" + "</td></tr>")
    else:
        test_report = ("<tr>" + " ".join(html_report)
                       + "</tr>" + "<tr><td bgcolor = '#b3ffff'> Testcase Summary </td><td bgcolor = '#00b33c'>"
                       + str(pass_count) + " : Attributes Passed " + "</td><td bgcolor = '#ff6666'>"
                       + str(fail_count) + " : Attributes Failed" + "</td></tr><tr><td bgcolor = '#b3ffff'>"
                       + "TestCase Status " + "</td><td bgcolor = '#29a329'>" + "Pass" + "</td></tr>")
    return test_report


def test_parser(result):
    rn = result.split("|")
    fail_count = 0
    pass_count = 0
    for gt in rn:
        if gt == 'Fail':
            fail_count = fail_count + 1

        else:
            pass_count = pass_count + 1

    if fail_count > 0:
        test_report = "Failed"
    else:
        test_report = "Pass"
    return test_report


def parse_test_sql(bucket, sql_script_paths, arn=None):
    sql_dictionary = {}
    file_list = su.list_s3_objects(bucket, sql_script_paths)
    sql_query_built = []
    for file in file_list:
        print(f"\n{current_datetime()} :: main :: info - file list ..." + file)

        test_query_content = su.get_s3_object(bucket, file, arn).decode('utf-8')

        sql_list = test_query_content.replace("\r", "").split("\n")
        for sql in sql_list:
            if sql != "":
                sql = sql.replace("\r", "").replace("\n", "")
                if "Testcase ID" in sql:
                    test_id = sql.replace("\n", "").replace("Testcase ID : ", "").strip()
                if "Testcase ID" not in sql or "sql_start" not in sql or "sql_end" not in sql:
                    sql_query_built.append(sql)
                if "sql_start" in sql or "Testcase ID" in sql:
                    sql_query_built = []
                if "sql_end" in sql:
                    t_sql = "  ".join(sql_query_built).replace('sql_end', '')
                    sql_dictionary[test_id] = t_sql

    return sql_dictionary


def parse_testcases(s3_config_file,
                    player='',
                    psrc_name='',
                    psanity='',
                    pcdm_table='',
                    psubject='Test Automation',
                    preportname='Test_Automation',
                    pvalidation_type="",
                    arn=None,
                    table_prefix=None):
    process_start_time = datetime.now()

    bucket, config_file = su.s3_path_to_bucket_key(s3_config_file)
    test_status = {}
    param_data = su.get_s3_object(bucket, config_file, arn)
    param_contents = json.loads(param_data)

    from_email_id = param_contents['qc_integration']['from_email']
    to_email_ids = param_contents["qc_integration"]["email_id_list"]
    email_subject = "Predictive Patient project  :- " + psubject + f" Testcase Validation Report "
    email_content = "<p>Hi Team,<br><br> Test Queries are executed successfully; " \
                    "Test Report is generated for the same.<br><br>Thank you,<br>ConcertAI Team</p>"

    layer = player.lower()
    if layer in ["cai_creation", "cleansed", "ing_with_cai", "ingestion", "masked", "staging", "test_dg", ]:
        redshift_db = param_contents['s3_to_redshiftdb']['redshift_db']
        aws_region = param_contents['s3_to_redshiftdb']['aws_region']
        secret_manager = param_contents['s3_to_redshiftdb']['secret_manager_db']
        secret_name = param_contents['qc_integration']["env1"]['secret_name']
        secret_key = param_contents['qc_integration']["env1"]['secret_key']
    elif layer in ["icdm", "masked_filtered", "outbound", ]:
        redshift_db = param_contents["s3_to_redshiftdb"]["icdm"]["redshift_db"]
        aws_region = param_contents['s3_to_redshiftdb']['aws_region']
        secret_manager = param_contents["s3_to_redshiftdb"]["icdm"]["secret_manager_db"]
        secret_name = param_contents['qc_integration']["env2"]['secret_name']
        secret_key = param_contents['qc_integration']["env2"]['secret_key']
    else:
        raise Exception(f"layer '{layer}' not handled in current version")

    fail_count = 0
    pass_count = 0
    pending_count = 0

    final_test_report = [
        "<table border='1'><tr bgcolor='#00ccff' ><td>Total "
        "Testcases</td><td>Pass</td><td>Fail</td><td>Pending</td><td>Elapsed_Time</td></tr>"]
    html_report_summary_passed = [
        "<table border='1' id='passed_summary_report'><tr "
        "bgcolor='#00ccff'><td>Testcase_ID</td><td>Result</td><td>Elapsed_Time</td><td>Comments</td></tr>"]
    html_report_summary_failed = [
        "<table border='1' id='failed_summary_report' ><tr "
        "bgcolor='#00ccff'><td>Testcase_ID</td><td>Result</td><td>Elapsed_Time</td><td>Comments</td></tr>"]
    html_report_summary_pending = [
        "<table border='1' id='pending_summary_report'><tr "
        "bgcolor='#00ccff'><td>Testcase_ID</td><td>Result</td><td>Elapsed_Time</td><td>Comments</td></tr>"]
    html_report = []
    csv_report = []
    test_status = {}
    date_time = datetime.now().strftime('%Y%m%d%H%M%S')

    html_report_file_name = f"{preportname}_{player}_{psrc_name}_{pvalidation_type.replace(' ', '_')}_validation_report_{date_time}.html"
    csv_report_file_name = preportname + "_" + "Validation_Report" + "_" + date_time + ".csv"
    title = "<h1 style='text-align: center;'><span style='text-decoration: underline;'><em>" \
            + " " + preportname.upper() + " " + " VALIDATION  REPORT " + "generated on " \
            + str(datetime.now().strftime("%m/%d/%Y %H:%M")) + "</em></span></h1><p><p>"

    bucket, test_config = su.s3_path_to_bucket_key(param_contents['qc_integration']['qc_config_file'])
    bucket, qc_sql_path = su.s3_path_to_bucket_key(param_contents['qc_integration']['qc_sql_path'])
    bucket, qc_results_path = su.s3_path_to_bucket_key(param_contents['qc_integration']['qc_results_path'])

    print(f"\n{current_datetime()} :: main :: info - test_config ..." + test_config)
    print(f"\n{current_datetime()} :: main :: info - qc_sql_path ..." + qc_sql_path)
    print(f"\n{current_datetime()} :: main :: info - qc_results_path ..." + qc_results_path)

    cmd_table = {}
    src_name = {}
    testcase_to_execute = []
    expected_result = {}
    sql_to_execute = {}
    test_description = {}
    test_entity = {}

    config_content = su.get_s3_object(bucket, test_config, arn)
    ts = pd.read_csv(io.StringIO(config_content.decode('utf-8'))).fillna('')
    # ts["src_name"] = ts["src_name"].str.split(',')
    # ts = ts.explode('src_name')

    if psrc_name.strip() != "":
        ts = ts.loc[(ts['src_name'].str.strip().str.lower() == psrc_name.strip().lower())]
    if pcdm_table.strip() != "":
        ts = ts.loc[(ts['cdm_table'].str.strip().str.lower() == pcdm_table.strip().lower())]
    if player.strip() != "":
        ts = ts.loc[(ts['layer'].str.strip().str.lower() == player.strip().lower())]
    if psanity.strip() != "":
        ts = ts.loc[(ts['Sanity'].str.strip().str.lower() == psanity.strip().lower())]
    if pvalidation_type.strip() != "":
        ts = ts.loc[(ts['validation_type'].str.strip().str.lower() == pvalidation_type.strip().lower())]
    if ts.empty:
        return {"html": "", "test_case_results": {}}

    print(f"{current_datetime()} :: main :: info - ts count	: '{ts.count()}'")
    sql_dictionary = parse_test_sql(bucket, qc_sql_path)
    # print(f"{current_datetime()} :: main :: info - sql_dictionary	: '{sql_dictionary}'")

    print(f"\n{current_datetime()} :: main :: info - fetching secret manager credentials ...")
    try:
        db_credentials = ru.get_credentials_from_sm(secret_manager, aws_region)
        json_creds = json.loads(db_credentials["SecretString"])

        redshift_host = json_creds['host'].lstrip()
        redshift_port = json_creds['port']
        redshift_user = json_creds['user']
        redshift_password = json_creds['password']
    except Exception as err:
        print(f"{current_datetime()} :: main :: error - failed to fetch secret manager credentials")
        print("error details : ", err)
        raise err
    else:
        print(f"{current_datetime()} :: main :: info - successfully fetched secret manager credentials\n")
        print(f"{current_datetime()} :: main :: info - redshift_host	: '{redshift_host}'")
        print(f"{current_datetime()} :: main :: info - redshift_port	: '{redshift_port}'")
        print(f"{current_datetime()} :: main :: info - redshift_user	: '{redshift_user}'")
        print(f"{current_datetime()} :: main :: info - redshift_db	    : '{redshift_db}'")

    redshift_connection = ru.get_redshift_connection(redshift_host, redshift_port, redshift_user, redshift_password,
                                                     redshift_db)
    redshift_cursor = redshift_connection.cursor()

    for row in ts.itertuples():
        cmd_table[row.testcase_id] = str(row.cdm_table)
        src_name[row.testcase_id] = str(row.src_name)

        expected_result[row.testcase_id] = str(row.expected)
        testcase_to_execute.append(row.testcase_id)

        sql_to_execute[row.testcase_id] = row.sql_id.strip()
        test_description[row.testcase_id] = str(row.TestCase)
        test_entity[row.testcase_id] = str(row.cdm_table)

    print(f"{current_datetime()} :: main :: info - testcase_id	: '{testcase_to_execute}'")

    for testcaseid in testcase_to_execute:
        if sql_to_execute[testcaseid] in sql_dictionary:
            html_report.append(
                "<br><table border = '1' bgcolor='#AED6F1' width='100%' " + " id='" + testcaseid + "_detailed_report"
                + "' >")
            html_report.append("<h3><i><B>" "Testcase ID :- " + "</td><td>" + testcaseid + "</i></b></h3><br>")
            html_report.append("<br><table border = '1' bgcolor='#E5E8E8' width='100%' >")
            html_report.append(
                "<tr bgcolor='#B4DCDE'><td>" + "CDM Table Name :- " + "</td><td>" + test_entity[
                    testcaseid] + "</td></tr>")
            html_report.append(
                "<tr bgcolor='#B4DCDE'><td>" + "SQL ID" + "</td><td>" + sql_to_execute[testcaseid] + "</td></tr>")
            html_report.append("<tr bgcolor='#B4DCDE'><td>" + "Testcase Description" + "</td><td>" + test_description[
                testcaseid] + "</td></tr>")
            html_report.append("<tr bgcolor='#B4DCDE'><td>" + "Testcase Expected" + "</td><td>" + expected_result[
                testcaseid] + "</td></tr>")
            html_report.append("</table>")
            html_report.append(
                "<table border = '1' bgcolor='#E5E8E8' width='100%'" + " id='" + testcaseid + "_test_query" + "'>")

            if table_prefix:
                html_report.append("<tr bgcolor='#D2B4DE'><td>"
                               + sql_dictionary[sql_to_execute[testcaseid]].
                               replace("<table_prefix>", table_prefix).replace('\n', ',<br>')
                               + "</td></tr></table>")
            else:
                html_report.append("<tr bgcolor='#D2B4DE'><td>"
                                   + sql_dictionary[sql_to_execute[testcaseid]].replace('\n', ',<br>')
                                   + "</td></tr></table>")

        start_time = datetime.now()
        try:
            if sql_to_execute[testcaseid] not in sql_dictionary.keys():
                pending_count = pending_count + 1
                html_report.append("<h5>Test SQL Exception </h5>")
                html_report.append(
                    "<table border = '1' bgcolor='#E5E8E8' width='100%'" + " id='" + testcaseid + "_test_query" + "'>")
                html_report.append(
                    "<tr bgcolor='#D2B4DE'><td>" + "Pending :- " + "</td><td>" + "SQL not found" + "</td></tr></table>")
                html_report.append("</table>")
                html_report_summary_pending.append(
                    '<tr bgcolor="#F4D03F"><td>' + testcaseid + '</td><td>' + '<a href="#' + testcaseid
                    + '_detailed_report">' + 'Pending' + '</td><td>' + "SQL not found" + '</td></tr>')
                csv_report.append(
                    testcaseid + "," + test_entity[testcaseid] + "," + test_description[testcaseid] + "," +
                    expected_result[testcaseid] + "," + "Failed")
                test_status[testcaseid] = 'Pending'
            elif sql_to_execute[testcaseid] in sql_dictionary.keys():
                t_sql = sql_dictionary[sql_to_execute[testcaseid]]
                if table_prefix:
                    t_sql = t_sql.replace("<table_prefix>", table_prefix)
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} :: Start :: {testcaseid}")
                # redshift_cursor.execute("SET LOCAL statement_timeout = 5000")
                redshift_cursor.execute(t_sql)
                end_time = datetime.now()
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} :: end :: {testcaseid}")
                description = []
                for desc in redshift_cursor.description:
                    description.append(desc[0])
                hed = str('|'.join([str(i) for i in description]))
                row = redshift_cursor.fetchone()
                res = str('|'.join([str(i) for i in row]))

                # redshift_cursor.close()
                # redshift_connection.close()
                # redshift_connection = ru.get_redshift_connection(redshift_host, redshift_port, redshift_user,
                #                                                  redshift_password, redshift_db)
                # redshift_cursor = redshift_connection.cursor()

                if expected_result[testcaseid] == "all_zero":
                    rep = equal_to_zero(res)
                elif expected_result[testcaseid] == "ignore":
                    rep = str('|'.join([str("pass") for _ in row]))
                elif expected_result[testcaseid] == "all_one":
                    rep = equal_to_one(res)
                elif expected_result[testcaseid] == "anynumber":
                    rep = greater_than_one(res)
                else:
                    rep = compare_with_expected_value(expected_result[testcaseid], res)

                rel = report_parse(rep)
                test_result = testcase_result(rep)

                html_report.append("<table border = '1' bgcolor='#E5E8E8' width='100%' >")
                html_report.append("<tr bgcolor='#B2BABB'><td/>" + hed.replace("|", "</td><td>") + "</td></tr>")
                html_report.append("<tr><td/>" + res.replace("|", "</td><td>") + "</td></tr>")
                html_report.append("<tr bgcolor='#85C1E9'><td/>" + hed.replace("|", "</td><td>") + "</td></tr>")
                html_report.append(rel)
                html_report.append("<tr bgcolor='#D2B4DE'><td>" + "Elapsed Time :- " + "</td><td>" + str(
                    get_elapsed_time(start_time, end_time)) + "</td></tr></table>")
                # html_report.append('<p>' + elapsed_time + '<p>')
                html_report.append('<table bgcolor="#D8D8D8" border="1">')
                html_report.append("</table>")
                html_report.append("</td></tr></table>")

                if test_parser(rep) == 'Failed':
                    fail_count = fail_count + 1
                    html_report_summary_failed.append('<tr bgcolor="#F1948A"><td>'
                                                      + testcaseid + '</td><td>' + '<a href="#'
                                                      + testcaseid + '_detailed_report">'
                                                      + test_parser(rep) + '</td><td>'
                                                      + str(get_elapsed_time(start_time, end_time)) + '</td><td>'
                                                      + '' + '</td></tr>')
                else:
                    pass_count = pass_count + 1
                    html_report_summary_passed.append('<tr bgcolor="#00cc44"><td>'
                                                      + testcaseid + '</td><td>' + '<a href="#'
                                                      + testcaseid + '_detailed_report">'
                                                      + test_parser(rep) + '</td><td>'
                                                      + str(get_elapsed_time(start_time, end_time)) + '</td><td>'
                                                      + "" + '</td></tr>')
                csv_report.append(
                    testcaseid + "," + test_entity[testcaseid] + "," + test_description[testcaseid] + "," +
                    expected_result[testcaseid] + "," + test_result)
                test_status[testcaseid] = test_result
        except Exception as err:
            fail_count = fail_count + 1
            end_time = datetime.now()
            html_report.append(
                "<table border = '1' bgcolor='#E5E8E8' width='100%'" + " id='" + testcaseid + "_test_query" + "'>")
            html_report.append(
                "<tr bgcolor='#D2B4DE'><td>" + "Exception :- " + "</td><td>" + str(err) + "</td></tr></table>")
            html_report.append("</table>")
            html_report_summary_failed.append('<tr bgcolor="#F1948A"><td>'
                                              + testcaseid + '</td><td>' + '<a href="#'
                                              + testcaseid + '_detailed_report">' + 'Failed' + '</td><td>'
                                              + str(get_elapsed_time(start_time, end_time)) + '</td><td>'
                                              + str(err) + '</td></tr>')

            csv_report.append(
                testcaseid + "," + test_entity[testcaseid] + "," + test_description[testcaseid] + "," + expected_result[
                    testcaseid] + "," + "Failed")
            test_status[testcaseid] = "Failed"
            redshift_cursor.close()
            redshift_connection.close()
            redshift_connection = ru.get_redshift_connection(redshift_host, redshift_port, redshift_user,
                                                             redshift_password, redshift_db)
            redshift_cursor = redshift_connection.cursor()

            pass
    redshift_cursor.close()
    redshift_connection.close()

    process_end_time = datetime.now()
    final_test_report.append("<tr><td>" + str(
        pass_count + fail_count + pending_count) + "</td><td bgcolor='#66ffc2'>" + '<a href="' + '#passed_summary_report">' + str(
        pass_count) + "</td><td bgcolor='#FE642E'>" + '<a href="' + '#failed_summary_report">' + str(
        fail_count) + "</td><td bgcolor='#F4D03F'>" + '<a href="' + '#pending_summary_report">' + str(
        pending_count) + "</td><td>" + get_elapsed_time(process_start_time, process_end_time) + "</td></tr>")
    final_test_report.append("</table><br>")
    html_report_summary_passed.append("</table>")
    html_report_summary_failed.append("</table>")
    html_report_summary_pending.append("</table>")
    complete_html_report = title \
                           + "<body bgcolor='#E8F8F5' width='100%' >" \
                           + "<h2><i><b> Test Summary Report <b></i></h2>" \
                           + "\n".join(final_test_report) + "<h2><i><b> Failed Test Report </b></i></h2>" \
                           + "\n".join(html_report_summary_failed) + "<h2><i><b> Pass Test Report </b></i></h2>" \
                           + "\n".join(html_report_summary_passed) + "<h2><i><b> Pending Test Cases </b></i></h2>" \
                           + "\n".join(html_report_summary_pending) + "<h2><i><b> Detailed Report </b></i></h2>" \
                           + "\n".join(html_report) + "</body>"

    su.put_s3_object(bucket, qc_results_path + html_report_file_name, complete_html_report)
    su.put_s3_object(bucket, qc_results_path + csv_report_file_name, "\n".join(csv_report))

    try:
        sg_credentials = secrets_client.get_secret_value(SecretId=secret_name)
        json_creds = json.loads(sg_credentials["SecretString"])
        analytics_creds = json_creds[secret_key]
    except Exception as err:
        print("error details 1 : ", err)
        raise err

    message = Mail(
        from_email=from_email_id,
        to_emails=to_email_ids,
        subject=email_subject,
        html_content=email_content)

    obj = s3_resource.Object(bucket, qc_results_path + html_report_file_name)
    body = obj.get()['Body'].read()

    encoded = base64.b64encode(body).decode()
    _attachment = Attachment()
    _attachment.file_content = FileContent(encoded)
    _attachment.file_type = FileType('application/html')
    _attachment.file_name = FileName(html_report_file_name)
    _attachment.disposition = Disposition('attachment')
    message.attachment = _attachment

    try:
        sendgrid_client = SendGridAPIClient(analytics_creds)
        response = sendgrid_client.send(message)
        print(response)
    except Exception as e:
        traceback.print_exc()
        print("error details 2 : ", e)

    return {"html": html_report_file_name, "test_case_results": test_status}  # TODO
