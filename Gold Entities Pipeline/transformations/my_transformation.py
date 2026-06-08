from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

#EXPECTATIONS

expec_coaches = {
    "rule1" : "code is not null",
    "rule2" : "current is True"
}

expec_nocs = {
    "rule1" : "code is not null"
}

expec_events = {
    "rule1" : "event is not null"
}

#COACHES TABLE

@dlt.table
def source_table_coaches():
    df = spark.readStream.table("olympics.silver.coaches")
    return df

@dlt.view

def view_coaches():
    df = spark.readStream.table("LIVE.source_table_coaches")
    return df

@dlt.table
@dlt.expect_all(expec_coaches)
def coaches():
    df = spark.readStream.table("LIVE.view_coaches")
    return df

 # NOCS TABLE

@dlt.view

def view_nocs():
    df = spark.readStream.table("olympics.silver.nocs")
    return df

@dlt.table
@dlt.expect_all_or_drop(expec_nocs)
def nocs():
    df = spark.readStream.table("LIVE.view_nocs")
    return df

# EVENTS TABLE

@dlt.view
@dlt.expect_all(expec_events)
def view_events():
    df = spark.readStream.table("olympics.silver.events")
    return df

@dlt.table
def events():
    df = spark.readStream.table("LIVE.view_events")
    return df

#ATELETES TABLE

#CDC - APPLY CHANGES DLT

@dlt.view

def source_atheletes():
    df = spark.readStream.table("olympics.silver.atheletes")
    return df

dlt.create_streaming_table("atheletes")

dlt.apply_changes(
    target = "atheletes",
    source = "source_atheletes",
    keys = ["athelete_id"],
    sequence_by = col("height"),
    stored_as_scd_type = 1
)
