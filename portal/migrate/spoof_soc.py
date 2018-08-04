import pandas
import pickle
import influx
import os
# import matplotlib.pyplot as plt

SITE_ID = "wfla"

INFLUX_URL = os.environ.get("INFLUX_URL", "http://localhost:8086")


with open("perfest_patcheddd.pickle", "rb") as opener:
    fields, values = pickle.load(opener)

starting_ts = "2018-07-21 12:45"
ending_ts = "2018-07-21 20:45"

index = pandas.date_range(starting_ts, ending_ts, freq="15T")
empty = pandas.DataFrame([1] * len(index), index=index)

frame = pandas.DataFrame(values, columns=fields)
frame = frame[["time", "building.offset.kW", "rb.state_of_charge.fraction"]]
frame.index = pandas.to_datetime(frame["time"], unit="s")
frame = frame.resample("15T").mean()

total = pandas.concat([frame, empty], axis=1).fillna(method="bfill")

total["rb.state_of_charge.fraction"][0] = 0.7654382

for i in range(1, len(total)):
    offset = total["building.offset.kW"][i]
    previous_soc = total["rb.state_of_charge.fraction"][i-1]

    if offset < 0:
        sign = -1
        total["rb.state_of_charge.fraction"][i] = \
            (sign * (offset - 3)**2) * 0.00322**2 + previous_soc
    else:
        sign = 1
        total["rb.state_of_charge.fraction"][i] = \
            (sign * (offset - 3)**2) * 0.0022**2 + previous_soc

# frame = frame.resample("15T").mean()
total = total[["rb.state_of_charge.fraction"]]
total["time"] = [int(idx.value // 1e9) for idx in total.index]
total = total.dropna()
# convert to floats
vals = total.values.tolist()
vals = [[float(v) for v in lst] for lst in vals]
client = influx.InfluxDB(url=INFLUX_URL, precision="s", timeout=300)

print(total)

# print(vals)
# print(total.columns)

# client.write_many(
#     database="cwp",
#     measurement="tmp_07_21",
#     fields=list(total.columns),
#     values=vals,
#     tags={"site_id": SITE_ID},
#     time_field="time")


# plt.figure()
# frame["rb.state_of_charge.fraction"].plot()
# plt.show()
