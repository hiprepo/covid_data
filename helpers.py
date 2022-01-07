import datetime


def cd_to_datetime(calendar_date):
    """
    :param calendar_date: A calendar date in YYYY-mm-DD format.
    :return: A `datetime` corresponding to the given calendar date and time.
    """
    return datetime.datetime.strptime(calendar_date, "%Y-%m-%d")


def datetime_to_str(dt):
    """Convert a Python datetime into a human-readable string.
    :param dt: A Python datetime.
    :return: That datetime, as a human-readable string.
    """
    return datetime.datetime.strftime(dt, "%Y-%m-%d")
