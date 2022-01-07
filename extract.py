import csv
import json
import datetime

from models import CDCTrackingObject
from helpers import datetime_to_str
from collections import defaultdict


def load_cdc_data(data_csv_path):
    """Read CDC tracking information from a CSV file.

    :param data_csv_path: A path to a CSV file containing data about cdc tracking data.
    :return: A collection of `Covid Tracking Object`s.
    """
    with open(data_csv_path, "r") as infile:
        reader = csv.DictReader(infile)
        cdcTrackingObjs = []
        for line in reader:
            line["date"] = line["date"]
            line["state"] = line["state"]
            line["death"] = int(line["death"]) if line["death"] else 0
            line["deathIncrease"] = int(line["deathIncrease"]) if line["deathIncrease"] else 0
            line["hospitalizedCumulative"] = int(line["hospitalizedCumulative"]) if line["hospitalizedCumulative"] else 0
            line["hospitalizedCurrently"] = int(line["hospitalizedCurrently"]) if line["hospitalizedCurrently"] else 0
            line["inIcuCurrently"] = int(line["inIcuCurrently"]) if line["inIcuCurrently"] else 0
            line["negative"] = int(line["negative"]) if line["negative"] else 0
            line["onVentilatorCumulative"] = int(line["onVentilatorCumulative"]) if line["onVentilatorCumulative"] else 0
            line["onVentilatorCurrently"] = int(line["onVentilatorCurrently"]) if line["onVentilatorCurrently"] else 0
            line["positive"] = int(line["positive"]) if line["positive"] else 0
            line["totalTestResultsIncrease"] = int(line["totalTestResultsIncrease"]) if line["totalTestResultsIncrease"] else 0
            line["totalTestsAntibody"] = int(line["totalTestsAntibody"]) if line["totalTestsAntibody"] else 0
            line["totalTestsAntigen"] = int(line["totalTestsAntigen"]) if line["totalTestsAntigen"] else 0
            line["totalTestsViral"] = int(line["totalTestsViral"]) if line["totalTestsViral"] else 0
            try:
                cdcData = CDCTrackingObject(
                    date = line["date"],
                    state = line["state"],
                    death = line["death"],
                    deathIncrease = line["deathIncrease"],
                    hospitalizedCumulative = line["hospitalizedCumulative"],
                    hospitalizedCurrently = line["hospitalizedCurrently"],
                    inIcuCurrently = line["inIcuCurrently"],
                    negative = line["negative"],
                    onVentilatorCumulative = line["onVentilatorCumulative"],
                    onVentilatorCurrently = line["onVentilatorCurrently"],
                    positive = line["positive"],
                    totalTestResultsIncrease = line["totalTestResultsIncrease"],
                    totalTestsAntibody = line["totalTestsAntibody"],
                    totalTestsAntigen = line["totalTestsAntigen"],
                    totalTestsViral = line["totalTestsViral"],
                )
            except Exception as e:
                print(e)
            else:
                cdcTrackingObjs.append(cdcData)
    return cdcTrackingObjs

if __name__ == ('__main__'):
   objs = load_cdc_data('data/all-states-history.csv')

   data_by_date = defaultdict(list)
   data_by_state = defaultdict(list)
   for _, d in enumerate(objs):
      data_by_date[datetime_to_str(d.date)].append(d)
      data_by_state[d.state].append(d)

   print(len(data_by_date))
   print(data_by_date['2021-03-07'])
   print(len(data_by_date['2021-03-07']))

   print(data_by_date['2020-03-08'])
   print(len(data_by_date['2020-03-08']))

   print(f'total number of data for WI: {len(data_by_state["WI"])}')
   print(data_by_state['WI'][0])
   print(data_by_state['WI'][-1])
  
   print(f'total number of data for CA: {len(data_by_state["CA"])}')
   for _, d in enumerate(data_by_state['CA']):
      print(d)

   print(len(objs))
   print(objs[10000])
   print(repr(objs[10000]))
