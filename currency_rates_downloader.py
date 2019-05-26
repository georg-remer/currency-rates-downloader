""" This module downloads currency exchange rates, making requests to
API of Central Bank of Russia and National Bank of Ukraine.

Verison: 1.0.0

"""
import datetime
from email.message import EmailMessage
import pprint
import smtplib
import xml.etree.ElementTree as ET

import psycopg2
import requests

from settings import DATABASE, EMAIL


def get_cursor(db_host, db_port, db_user, db_password, db_name):
    """ Return database connection and cursor.

    Arguments:
    db_host
    db_port
    db_user
    db_password
    db_name

    Return:
    connection 
    cursor

    """
    connection = psycopg2.connect(
        host=db_host, port=db_port, user=db_user,
        password=db_password, dbname=db_name
    )
    cursor = connection.cursor()
    return (connection, cursor)


def get_url_list(
        cursor, var_in_crncy_id,
        var_check_min_threshold=True,
        var_min_threshold=(
            datetime.datetime.now().date()-datetime.timedelta(days=7)),
        var_max_date=datetime.datetime.now().date()):
    """ Get a list of URLs to be called.

    Arguments:
    cursor -- cursor returned by get_cursor method;
    var_in_crncy_id -- pass 2 or 8 (ID for RUB and UAH in database)
        to get a list of URLs for Central Bank of Russia or
        National Bank of Ukraine;
    var_check_min_threshold -- set to False if currency exchange rates
        for all of the available dates should be downloaded, set to True
        (this is the default) to specify the date to start downloading
        rates from (is used to exclude unnecessary data);
    var_min_threshold -- if previous argument is set to True, specify
        this minimum date (the default is minus 7 days from today);
    var_max_date -- the latest date to download rates up to, e.g.
        '11-30-2018'::date (the default is today).

    Return:
    url_list -- a list of URLs to be called at the next step.

    """
    print('Getting URL list...')
    cursor.execute(
        "SELECT * FROM currency.f_crncy_downldr_return_url(%s, %s, %s, %s)",
        (
            var_in_crncy_id, var_check_min_threshold,
            var_min_threshold, var_max_date
        ))
    url_list = {}
    row = cursor.fetchone()
    while row is not None:
        url_list[row[0]] = row[1]
        row = cursor.fetchone()
    cursor.execute("COMMIT")
    return url_list


def get_crncy_list(
        cursor, var_in_crncy_id,
        var_check_min_threshold=True,
        var_min_threshold=(
            datetime.datetime.now().date()-datetime.timedelta(days=7))):
    """ Get a list of currencies, exchange rates for which are needed
    to be downloaded.

    Arguments:
    cursor -- cursor returned by get_cursor method;
    var_in_crncy_id -- pass 2 or 8 (ID for RUB and UAH in database)
        to get a list of URLs for Central Bank of Russia or
        National Bank of Ukraine;
    var_check_min_threshold -- set to False if currency exchange rates
        for all of the available dates should be downloaded, set to True
        (this is the default) to specify the date to start downloading
        rates from (is used to exclude unnecessary data);
    var_min_threshold -- if previous argument is set to True, specify
        this minimum date (the default is minus 7 days from today).

    Return:
    crncy_list -- a list of currencies to be checked upon at the
        next step.

    """
    print('Getting currency list...')
    cursor.execute(
        "SELECT * FROM currency.f_crncy_downldr_return_crncs(%s, %s, %s)",
        (var_in_crncy_id, var_check_min_threshold, var_min_threshold))
    crncy_list = {}
    row = cursor.fetchone()
    while row is not None:
        crncy_list[row[0]] = row[1]
        row = cursor.fetchone()
    cursor.execute("COMMIT")
    return crncy_list


def download_rates(url_list, crncy_list, var_in_crncy_id):
    """ Make requests for URLs to get exchange rates.

    Arguments:
    url_list -- a list of URLs to be requested as returned by get_url_list
        method;
    crncy_list -- a list of currencies, exchange rates for which are
        needed to be downloaded;
    var_in_crncy_id -- pass 2 or 8 (ID for RUB and UAH in database)
        to get a list of URLs for Central Bank of Russia or
        National Bank of Ukraine;

    Return:
    has_result_to_send -- True if anything has been saved to database
    email_body -- email body to be sent

    """
    email_body = "Downloaded exchange rates from CBR:\n\n" \
        if var_in_crncy_id == 2 else "Downloaded exchange rates from NBU :\n\n"
    has_result_to_send = False
    for key, value in url_list.items():
        date = key
        url = value
        email_body_date = ""
        try:
            print('Making request for url {}'.format(url))
            response = requests.get(url, timeout=30)
            response.raise_for_status()
        except requests.Timeout:
            print("ERROR timeout, url:", url)
        except requests.HTTPError as err:
            code = err.response.status_code
            print("ERROR url: {0}, code: {1}".format(url, code))
        except requests.RequestException:
            print("ERROR downloading url: ", url)
        else:
            email_body_date = email_body_date + save_data(
                date, response, var_in_crncy_id, crncy_list)
        if email_body_date != "":
            email_body = email_body + \
                "***** Exchange rates on date {} *****".format(date) + \
                "\n" + email_body_date + "\n"
            has_result_to_send = True
    return has_result_to_send, email_body


def save_data(date, response, var_in_crncy_id, crncy_list):
    """ Save data to database.

    Arguments:
    date -- date the request is made for;
    response -- returned after making successful request;
    var_in_crncy_id -- pass 2 or 8 (ID for RUB and UAH in database)
        to get a list of URLs for Central Bank of Russia or
        National Bank of Ukraine;
    crncy_list -- a list of currencies, exchange rates for which are
        needed to be downloaded;

    Return:
    email_body_date -- body for email, containing list of downloaded
        exchange rates.
    
    """
    root = ET.fromstring(response.text)
    # print(root)
    # print(root.tag)
    # print(root.attrib)
    email_body_date = ""

    for currency in root:
        if var_in_crncy_id == 2:
            # print(currency.tag, currency.attrib['ID'])

            # Currency identificator is present in list of currencie
            # and the URL date is fresher than the latest date when 
            # exchange rates where previously downloaded.
            if currency.attrib['ID'] in crncy_list:
                if date > crncy_list[currency.attrib['ID']] \
                    and date == datetime.datetime.strptime(
                        root.attrib['Date'], "%d.%m.%Y").date():
                    crncy = {'Identificator': currency.attrib['ID'],}
                    for detail in currency:
                        crncy[detail.tag] = detail.text
                    print('Saving {} on {} to DB...'.format(
                        crncy['Identificator'], date))
                    cur.execute(
                        "SELECT * FROM currency.f_crncy_downldr_save_rate(\
                                %s, %s, %s, %s, %s)",
                        (
                            var_in_crncy_id, 
                            crncy['Identificator'],
                            date,
                            crncy['Nominal'],
                            float(crncy['Value'].replace(',', '.'))))
                    cur.execute("COMMIT")
                    print("Successfully done!")
                    email_body_date = email_body_date + "{}: {}/{}".format(
                        crncy['CharCode'],
                        crncy['Value'],
                        crncy['Nominal']
                    ) + "\n"
        else:
            crncy = {}
            for detail in currency:
                crncy[detail.tag] = detail.text
            if crncy['r030'] in crncy_list \
                    and date > crncy_list[crncy['r030']]:
                print('Saving {} on {} to DB...'.format(crncy['r030'], date))
                cur.execute(
                    "SELECT * FROM currency.f_crncy_downldr_save_rate(\
                        %s, %s, %s, %s, %s)",
                    (
                        var_in_crncy_id,
                        crncy['r030'],
                        date,
                        1,
                        float(crncy['rate'].replace(',', '.'))))
                cur.execute("COMMIT")
                print("Successfully done!")
                email_body_date = email_body_date + "{}: {}/{}".format(
                    crncy['cc'],
                    crncy['rate'],
                    1
                ) + "\n"
    return email_body_date


def send_email(
        e_body, e_subject, e_from, e_to, e_host, e_port, e_login, e_password):
    """ Send Email.

    Arguments:
    e_body
    e_subject
    e_from
    e_to
    e_host
    e_port
    e_login
    e_password

    """
    msg = EmailMessage()
    msg.set_content(e_body)
    msg['Subject'] = e_subject
    msg['From'] = e_from
    msg['To'] = e_to

    s = smtplib.SMTP(e_host, e_port)
    s.starttls()
    s.login(e_login, e_password)
    s.send_message(msg)
    s.quit()


def cleanup(cursor, connection):
    """ Close cursor and connection to database.

    Arguments:
    cursor
    connection

    """
    cursor.close()
    connection.close()


if __name__ == "__main__":

    # Use Russian ruble ID to make requests to API of Central Bank of
    # Russia or use Ukrainian hryvnia ID to  make requests to API of
    # National Bank of Ukraine. These IDs are from the database.
    ID_CBR = 2
    ID_NBU = 8

    # Set these to specify data range for downloading rates
    CHECK_MIN_THRESHOLD = True
    MIN_DATE = datetime.date(2019, 5, 5)
    MAX_DATE = datetime.datetime.now().date()

    print("***** Started my job at " + str(datetime.datetime.today()) + " *****")

    # Establish database connection and get cursor
    conn, cur = get_cursor(
        DATABASE['HOST'], DATABASE['PORT'], DATABASE['USER'],
        DATABASE['PASSWORD'], DATABASE['NAME']
    )

    # Get currency exchange rates from Central Bank of Russia
    url_list = get_url_list(
        cur, ID_CBR, CHECK_MIN_THRESHOLD, MIN_DATE, MAX_DATE)
    crncy_list = get_crncy_list(cur, ID_CBR, CHECK_MIN_THRESHOLD, MIN_DATE)
    cbr_has_result_to_send, email_body_cbr = download_rates(
        url_list, crncy_list, ID_CBR)

    # Get currency exchange rates from National Bank of Ukraine
    url_list = get_url_list(
        cur, ID_NBU, CHECK_MIN_THRESHOLD, MIN_DATE, MAX_DATE)
    crncy_list = get_crncy_list(cur, ID_NBU, CHECK_MIN_THRESHOLD, MIN_DATE)
    nbu_has_result_to_send, email_body_nbu = download_rates(
        url_list, crncy_list, ID_NBU)

    # Form Email body
    email_body = ""
    if cbr_has_result_to_send and not nbu_has_result_to_send:
        email_body = email_body + email_body_cbr
    elif cbr_has_result_to_send and nbu_has_result_to_send:
        email_body = email_body_cbr + '\n\n' + email_body_nbu
    elif not cbr_has_result_to_send and nbu_has_result_to_send:
        email_body = email_body + email_body_nbu

    # Send Email if any exchange rates have been downloaded
    if email_body != "":
        send_email(
            email_body, EMAIL['SUBJECT'], EMAIL['FROM'], EMAIL['TO'],
            EMAIL['HOST'], EMAIL['PORT'], EMAIL['LOGIN'], EMAIL['PASSWORD']
        )

    # Close database connection and cursor
    cleanup(cur, conn)

    print("***** Finished my job at " + str(datetime.datetime.today()) + \
        " *****\n")
