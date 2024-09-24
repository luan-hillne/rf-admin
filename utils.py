from datetime import datetime, timedelta


def get_curr_dt():
    utc_now = (datetime.now() + timedelta(hours=7)).strftime(f'%Y-%m-%d %H:%M:%S')
    return utc_now


def write_log(message, log_type, log_time=True):
    fopen = open(f'logs/{log_type}.log', 'a', encoding='utf-8')
    if log_time:
        fopen.write(f'{get_curr_dt()} - {message}\n')
    else:
        fopen.write(f'{message}\n')
    fopen.close()
    return

def split_three(text: str):
    idx1 = text.find(' ')
    idx2 = text.find(' ', idx1 + 1)

    if "not in" in text:
        idx2 = text.find(' ', idx2 + 1)
    return (text[:idx1], text[idx1+1 : idx2], text[idx2+1:])