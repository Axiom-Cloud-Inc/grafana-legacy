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

SITE_ID = os.environ.get("SITE_ID", "wfla")

START = pandas.to_datetime(MIGRATION_START)
END = pandas.to_datetime(MIGRATION_END)

PERFEST_PICKLE = "perfest_patcheddd.pickle"

RBIMAGE_FIELDS = (
    'MODE("rbCur") AS "rbCur", '
    'MEAN("react.target_kw") + 29 AS "react.target_kw"')

PERFEST_FIELDS = (
    'MEAN("rbCur_num") AS "rbCur_num", '
    'MEAN("building.actual.power.kW") AS "building.actual.power.kW", '
    'MEAN("building.baseline.power.kW") AS "building.baseline.power.kW", '
    'MEAN("building.offset.kW") AS "building.offset.kW", '
    'MEAN("rb.state_of_charge.fraction") AS "rb.state_of_charge.fraction"'
    )

PERFEST_FIELDS_LIST = [
    "building.actual.power.kW",
    "building.baseline.power.kW",
    "building.offset.kW",
    # "rb.state_of_charge.fraction",
    "rbCur_num"
    ]


# LEGACY_MAPPING = {
#     "building_actual_power"
#     }


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
        "cwp.autogen.rbimage",
        fields=RBIMAGE_FIELDS,
        where="time > {} AND time < {}".format(
            int(start.timestamp()), int(end.timestamp())),
        group_by="time(15m), site_id")
    print("rbimage migration count", count)


def write_old_perfest():
    client = influx.InfluxDB(url=INFLUX_URL, precision="s", timeout=300)
    # client.create_database("cwp")
    with open(PERFEST_PICKLE, "rb") as opener:
        fields, values = pickle.load(opener)

    frame = pandas.DataFrame(values, columns=fields)
    frame = frame[PERFEST_FIELDS_LIST + ["time"]]
    frame.index = pandas.to_datetime(frame["time"], unit="s")
    frame = frame.resample("15T").mean()
    frame["time"] = [int(idx.value // 1e9) for idx in frame.index]
    frame = frame.dropna()
    # convert to floats
    vals = frame.values.tolist()
    vals = [[float(v) for v in lst] for lst in vals]
    client.write_many(
        database="cwp",
        measurement="tmp_07_21",
        fields=list(frame.columns),
        values=vals,
        tags={"site_id": SITE_ID},
        time_field="time")

    # Return the oldest perfest written value for reference
    return pandas.to_datetime(values[-1][0], unit="s")


def select_perfest(start, end):
    client = influx.InfluxDB(url=INFLUX_URL, precision="s")
    count = client.select_into(
        "cwp.autogen.summary",
        "cwp.autogen.perfest",
        fields=PERFEST_FIELDS,
        where="time > {} AND time < {}".format(
            int(start.timestamp()), int(end.timestamp())),
        group_by="time(15m), site_id")
    print("perfest migration count", count)


def migrate_soc(start, end):
    """
    Migrate the SOC separately because it's actually "correct" in staging.
    """
    client = influx.InfluxDB(url=INFLUX_URL, precision="s")
    fields = \
        'MEAN("rb.state_of_charge.fraction") AS "rb.state_of_charge.fraction"'
    count = client.select_into(
        "cwp.autogen.summary",
        "cwp.autogen.perfest",
        fields=fields,
        where="time > {} AND time < {}".format(
            int(start.timestamp()), int(end.timestamp())),
        group_by="time(15m), site_id")
    print("soc count", count)


def write_spoofed_rbimage(start, end):
    """
    Write spoofed rbimage and perfest data to influx.
    """
    times = [int(v.value // 1e9) for v in pandas.date_range(start, end,
             freq="15T")]
    rbCur = ["DMT"] * len(times)
    target_kw = [(v + 1) * 300 for v in generate_sinusoid(times)]
    random_pressure = [(v + 20) * 300 for v in generate_sinusoid(times)]

    print("spoofed rbimage len", len(times))

    client = influx.InfluxDB(url=INFLUX_URL, precision="s", timeout=300)
    client.write_many(
        database="cwp",
        measurement="rbimage",
        fields=["time", "rbCur", "react.target_kw", "random.P"],
        values=list(zip(times, rbCur, target_kw, random_pressure)),
        tags={"site_id": SITE_ID},
        time_field="time")


def write_spoofed_perfest(start, end):
    """
    Write spoofed rbimage and perfest data to influx.
    """
    times = [int(v.value // 1e9) for v in pandas.date_range(start, end,
             freq="15T")]
    rbcur_num = generate_sinusoid(times)
    building_load = [(v + 2) * 150 for v in generate_sinusoid(times)]
    building_baseline = [(v + 2) * 150 - 50 for v in generate_sinusoid(times)]
    building_offset = [v * 50 for v in generate_sinusoid(times)]
    soc = [(v * 30) + 100 for v in generate_sinusoid(times)]
    print("spoofed perfest len", len(times))

    client = influx.InfluxDB(url=INFLUX_URL, precision="s", timeout=300)
    client.write_many(
        database="cwp",
        measurement="perfest",
        fields=["time"] + PERFEST_FIELDS_LIST,
        values=list(zip(
            times, rbcur_num, building_load, building_baseline,
            building_offset, soc)),
        tags={"site_id": SITE_ID},
        time_field="time")


def generate_sinusoid(times):
    """
    Generate sine with period 30m
    """
    sine = numpy.sin([r * 3.14 / 180 * 2 * 3.14 / 30 / 60 for r in times])
    return [float(v) for v in sine]


def main():
    # Get the rbimage fields from START to END that we care about.
    last_perfest_time = write_old_perfest()
    # select_rbimage(START, END)
    print("last_perfest_time", last_perfest_time)
    # select_perfest(last_perfest_time, END)


if __name__ == "__main__":
    loglevel = os.environ.get('LOG_LEVEL', 'DEBUG').upper()
    loglevel = getattr(logging, loglevel)

    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s",
                        level=loglevel)
    print("INFLUX_URL", INFLUX_URL)

    print(START)
    print(END)

    # REMOVE BEFORE USING ON THE ACTUAL DATABASE
    # write_spoofed_rbimage(START, END)
    # write_spoofed_perfest(START, END)
    ############################################

    main()
