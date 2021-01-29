import gspread
import datetime
import time
import random


def get_client():
    # folder with creds /Users/rafarui/.config/gspread
    gc = gspread.oauth()
    sh = gc.open("status_techfin_reprocess_dev")
    techfin_worksheet = sh.worksheet("status")
    return techfin_worksheet


def find_tenant(techfin_worksheet, domain):
    time.sleep(round(1 + random.random() * 3, 2))
    if techfin_worksheet is None:
        return
    try:
        match = techfin_worksheet.find(domain)
        return match
    except gspread.CellNotFound:
        return


def update_status(techfin_worksheet, row, status):
    time.sleep(round(1 + random.random() * 3, 2))
    col = 9
    techfin_worksheet.update_cell(row, col, status)


def get_sync_type(techfin_worksheet, row):
    time.sleep(round(1 + random.random() * 3, 2))
    col = 4
    return techfin_worksheet.cell(row, col).value


def update_task_id(techfin_worksheet, row, status):
    time.sleep(round(1 + random.random() * 3, 2))
    col = 8
    techfin_worksheet.update_cell(row, col, status)


def update_start_time(techfin_worksheet, row):
    time.sleep(round(1 + random.random() * 3, 2))
    col = 6
    techfin_worksheet.update_cell(row, col, str(datetime.datetime.utcnow())[:-7])


def update_end_time(techfin_worksheet, row):
    time.sleep(round(1 + random.random() * 3, 2))
    col = 7
    techfin_worksheet.update_cell(row, col, str(datetime.datetime.utcnow())[:-7])


def update_version(techfin_worksheet, row, version):
    time.sleep(round(1 + random.random() * 3, 2))
    col = 3
    techfin_worksheet.update_cell(row, col, version)
