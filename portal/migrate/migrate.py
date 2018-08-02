import os
import string

import datetime
import pandas

import influx

INFLUX_URL = os.environ.get("INFLUX_URL", 'http://127.0.0.1:8086')

# UTC
MIGRATION_START = os.environ.get("MIGRATION_START", "2018-07-20 00:00:00")
MIGRATION_END = \
    os.environ.get("MIGRATION_END", str(datetime.datetieme.utcnow()))

SITE_ID = os.environ.get("SITE_ID", "WFLA")

START = pandas.to_datetime(MIGRATION_START)
END = pandas.to_datetime(MIGRATION_END)

RBIMAGE_FIELDS = ["rbCur", "react.target_kw"]

PERFEST_FIELDS = [
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


def main():
    # Get the rbimage fields from START to END that we care about.
    client = influx.InfluxDB(url=INFLUX_URL)
    rbimage = client.select_into(
        database="cirrus",
        measurement="rbimage",
        fields=string.join(RBIMAGE_FIELDS, ", "),
        tags={"site_id": SITE_ID},
        where="time > {} AND time < {}".format(
            START.timestamp() * 1e9, END.timestamp() * 1e9),
        group_by="time(15m)")
    perfest = client.select_into(
        database="cirrus",
        measurement="rbimage",
        fields=string.join(PERFEST_FIELDS, ", "),
        tags={"site_id": SITE_ID},
        where="time > {} AND time < {}".format(
            START.timestamp() * 1e9, END.timestamp() * 1e9),
        group_by="time(15m)")

    rbimage = process_to_df(rbimage)
    # Add 29 kW to our buffer because that's what we've been setting internally
    rbimage["react.target_kw"] += 29

    perfest = process_to_df(perfest)
    output = pandas.concat([rbimage, perfest], axis=1)
    output["time"] = output.index

    # Write out to our new influx
    client.write_many(
        database="cwp",
        measurement="summary",
        fields=list(output.columns),
        values=output.values.tolist(),
        tags={"site_id": SITE_ID},
        time_field="time")


if __name__ == "__main__":
    main()
