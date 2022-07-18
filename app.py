import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import numpy as np
from flask import Flask, jsonify
from datetime import timedelta

############################################################
# set up database connection
############################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# reflect engine to current database
Base = automap_base()
#reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
stations = Base.classes.station
session = Session(engine)
#############################################################
# Flask setup
#############################################################
app = Flask(__name__)

#############################################################
# flask routes
#############################################################

@app.route('/')
def welcome():
    """Homepage"""
    """list of all available routes"""

    return (f"/api/v1.0/precipitation<br>"
        f"/api/v1.0/stations`<br>"
        f"/api/v1.0/tobs"

    )

###############################################################

# Convert the query results to a dictionary using `date` as the key and `prcp` as the value.
# Return the JSON representation of your dictionary.

@app.route("/api/v1.0/precipitation")
def precipitation():
    results = session.query(Measurement.date, Measurement.prcp).all()
    session.close()

    res = {}
    for date_, prcp in results:
        res[date_] = prcp
    return res

################################################################
# Return a JSON list of stations from the dataset

@app.route("/api/v1.0/stations")
def get_stations():


    station_results = session.query(stations.name, stations.station).all()

    # covert list of turples into normal list
    all_stations = list(np.ravel(station_results))

    session.close()

    #return list of station names

    return jsonify(all_stations)

######################################################################
# Query the dates and temperature observations of the most active station for the previous year of data.\
# Return a JSON list of temperature observations (TOBS) for the previous year.
#####################################################################
@app.route("/api/v1.0/tobs")
def get_tobs():
    # converting data from the measuremt table into a pandas dataframe
    measurement_df = pd.DataFrame(session.query(Measurement.station, Measurement.date, Measurement.tobs))

    #sorting values of of the measurement_df dataframe in descening order in the date columns
    measurement_df.sort_values("date", ascending=False)

    # converting the date column into a datetime object
    measurement_df['date'] = pd.to_datetime(measurement_df['date'], infer_datetime_format=True)

    # extracting the most recent date in the date column
    most_recent = measurement_df['date'].max()

    #find the most date of the day that is 1 year before the most recent date
    one_year_from_most_recent_date = most_recent - timedelta(days=365)

    # making a dataframe with data that is 1 year old or less
    one_year_old_data = measurement_df[measurement_df['date'] > one_year_from_most_recent_date]

    # determining the most active stations the last one year. \
    # with this code the most active station will be in row zero

    most_active_stations =  one_year_old_data.station.value_counts(ascending=False).sort_values().\
                            rename_axis('station').reset_index(name='total_counts').\
                            sort_values('total_counts', ascending=False )


    # Extracting the name code of the most active station using the iloc function from the\
    # most_active stations dataframe
    most_active_station = most_active_stations.iloc[0,0]

    #Querying reading of the most active station for the past year and converting it to a dictionary
    most_active_station_readings = one_year_old_data.query(f"station == '{most_active_station}'").to_dict()


    #most_active_stations_readings = most_active_stations_readings.to_dict()

    #= one_year_old_data.to_dict()
    print(measurement_df.head())
    print(most_active_station)
    print(f"most active station is {most_active_stations.iloc[0,0]}")
    print(most_active_stations)


    return ( one_year_old_data.to_dict() )

    #return jsonify(most_active_stations)

    #session.query(Measurement.date, measurement.tobs).\
            #filter(Measurement.date => '2016-08-23').group_by(Measurement.station).counts().desc().all
if __name__ == '__main__':
    app.run(debug=True)