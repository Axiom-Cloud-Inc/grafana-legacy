import os
import pickle
import datetime
import pandas
import numpy
import logging

import influx

INFLUX_URL = os.environ.get("INFLUX_URL", 'http://localhost:8086')

# UTC
MIGRATION_START = os.environ.get("MIGRATION_START", "2018-07-09 18:30:00")
MIGRATION_END = \
    os.environ.get("MIGRATION_END", str(datetime.datetime.utcnow()))

SITE_ID = os.environ.get("SITE_ID", "WFLA")

START = pandas.to_datetime(MIGRATION_START)
END = pandas.to_datetime(MIGRATION_END)

print(START)
print(END)

PERFEST_PICKLE = "perfest.pickle"

RBIMAGE_FIELDS = (
    'MODE("rbCur") AS "rbCur", '
    'MEAN("react.target_kw") + 29 AS "react.target_kw"')

PERFEST_FIELDS = (
    'MEAN("rbCur_num") AS "rbCur_num", '
    'MEAN("building.actual.power.kW") AS "building.actual.power.kW", '
    'MEAN("building.baseline.power.kW") AS "building.baseline.power.kW", '
    'MEAN("building.offset.kW") AS "building.offset.kW", '
    'MEAN("rb.state_of_charge.fraction") AS "rb.state_of_charge.fraction"')

PERFEST_FIELDS_LIST = [
    "building.actual.power.kW",
    "building.baseline.power.kW",
    "building.offset.kW",
    "rb.state_of_charge.fraction"]


def process_to_df(influx_return):
    default = [{'series': [{'columns': [], 'values': []}]}]
    data = influx_return.get('results', default)
    data = data and data[0] or default[0]
    data = data.get('series', default[0]['series'])
    data = data and data[0] or default[0]['series'][0]
    columns = data['columns']
    values = data['values']

    columns = columns[:]
    index = [v[0] for v in values]
    values = [v[:] for v in values]
    frame = pandas.DataFrame(values, columns=columns, index=index)
    return frame


def select_rbimage(start, end):
    client = influx.InfluxDB(url=INFLUX_URL, precision="s")
    count = client.select_into(
        "cwp.autogen.summary",
        "cirrus.autogen.rbimage",
        fields=RBIMAGE_FIELDS,
        where="time > {} AND time < {}".format(
            int(start.timestamp()), int(end.timestamp())),
        group_by="time(15m), site_id")
    print(count)


def write_old_perfest():
    client = influx.InfluxDB(url=INFLUX_URL, precision="s", timeout=300)
    client.create_database("cwp")
    with open(PERFEST_PICKLE, "rb") as opener:
        fields, values = pickle.load(opener)

    frame = pandas.DataFrame(values, columns=fields)
    frame = frame[PERFEST_FIELDS_LIST + ["time"]]
    frame.index = pandas.to_datetime(frame["time"], unit="s")
    frame = frame.resample("15T").mean()
    frame["time"] = [idx.value // 1e9 for idx in frame.index]
    frame = frame.dropna()
    client.write_many(
        database="cwp",
        measurement="summary",
        fields=list(frame.columns),
        values=frame.values.tolist(),
        tags={"site_id": SITE_ID},
        time_field="time")

    # Return the oldest perfest written value for reference
    return pandas.to_datetime(values[-1][0], unit="s")


def select_perfest(start, end):
    client = influx.InfluxDB(url=INFLUX_URL, precision="s")
    count = client.select_into(
        "cwp.autogen.summary",
        "cirrus.autogen.perfest",
        fields=PERFEST_FIELDS,
        where="time > {} AND time < {}".format(
            int(start.timestamp()), int(end.timestamp())),
        group_by="time(15m), site_id")
    print(count)


def write_spoofed_rbimage(start, end):
    """
    Write spoofed rbimage and perfest data to influx.
    """
    times = [v.value // 1e9 for v in pandas.date_range(start, end,
             freq="100S")]
    rbCur = ["DMT"] * len(times)
    target_kw = [r % 25 for r in times]
    random_pressure = [r % 490 for r in times]

    print("spoofed rbimage len", len(times))

    client = influx.InfluxDB(url=INFLUX_URL, precision="s", timeout=300)
    client.write_many(
        database="cirrus",
        measurement="rbimage",
        fields=["time", "rbCur", "react.target_kw", "random.P"],
        values=list(zip(times, rbCur, target_kw, random_pressure)),
        tags={"site_id": SITE_ID},
        time_field="time")


def write_spoofed_perfest(start, end):
    """
    Write spoofed rbimage and perfest data to influx.
    """
    times = [v.value // 1e9 for v in pandas.date_range(start, end,
             freq="100S")]
    rbcur_num = numpy.random.rand(len(times))
    building_load = numpy.sin([r % 25 for r in times]) + 2 * 150
    building_baseline = numpy.sin([r % 25 for r in times]) + 2 * 150 - 50
    building_offset = numpy.cos([r for r in times])
    soc = numpy.sin([r for r in times]) * 30 + 100

    print("spoofed perfest len", len(times))

    client = influx.InfluxDB(url=INFLUX_URL, precision="s", timeout=300)
    client.write_many(
        database="cirrus",
        measurement="perfest",
        fields=["time"] + PERFEST_FIELDS_LIST,
        values=list(zip(
            times, rbcur_num, building_load, building_baseline,
            building_offset, soc)),
        tags={"site_id": SITE_ID},
        time_field="time")


def main():
    # Get the rbimage fields from START to END that we care about.
    select_rbimage(START, END)
    last_perfest_time = write_old_perfest()
    print("last_perfest_time", last_perfest_time)
    select_perfest(last_perfest_time, END)


if __name__ == "__main__":
    loglevel = os.environ.get('LOG_LEVEL', 'DEBUG').upper()
    loglevel = getattr(logging, loglevel)

    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s",
                        level=loglevel)

    client = influx.InfluxDB(url=INFLUX_URL, precision="s")
    client.create_database("cwp")
    write_spoofed_rbimage(START, END)
    write_spoofed_perfest(START, END)
    main()
