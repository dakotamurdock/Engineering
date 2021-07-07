import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta


# Method to calculate rolling averages for Pason traces passed in
def average_trace(col):
    trace = df_pason_on_bottom[col]
    rolling_windows = trace.rolling(90)
    rolling_mean = rolling_windows.mean()
    rolling_mean = rolling_mean.reset_index()

    depthIn = int(rolling_mean.iloc[0, 0])
    depthOut = int(rolling_mean.iloc[len(rolling_mean.index.values) - 1, 0])
    BinRange = []

    for i in range(0, ((depthOut - depthIn) // 90) + 1):
        BinRange.append(i * 90 + depthIn)

    grouped = rolling_mean.groupby(pd.cut(rolling_mean['HOLE_DEPTH'], BinRange))[col].mean()
    print_data = grouped.to_numpy()
    p_depths = pd.DataFrame({'Depth': BinRange})
    p_depths = p_depths.astype('float64')
    p_trace = pd.DataFrame({col: print_data})

    rolling_trace = pd.concat([p_depths, p_trace], axis=1, join='inner')
    p_depths_footage = rolling_trace['Depth'] - depthIn + 90
    rolling_trace_export = pd.concat([p_depths_footage, p_trace], axis=1, join='inner')
    return rolling_trace_export


# Creating list to hold data imported from Snowflake pason well info table
pason_well_info_view = []

# Connecting to Snowflake
ctx = snowflake.connector.connect(
    user='redacted',
    password='redacted',
    account='redacted',
    authenticator='redacted',
    database='DRILLING_MART',
    )

# Create a cursor object used to hold data from Snowflake
cur = ctx.cursor()

# Querying Snowflake
try:
    cur.execute("SELECT API, WELL_NAME, WELL_INFO_ID, SPUD, RELEASE "
                "FROM PASON_TEST.WELL_INFO_VIEW "
                "ORDER BY WELL_INFO_ID")
    for (API, WELL_NAME, WELL_INFO_ID, SPUD, RELEASE) in cur:
        pason_well_info_view.append((API, WELL_NAME, WELL_INFO_ID, SPUD, RELEASE))

# Closing connection to Snowflake
finally:
    cur.close()

# Converting list with Snowflake data into Pandas DataFrame
df_pason_well_info_view = pd.DataFrame(pason_well_info_view, columns=['API', 'WELL_NAME', 'WELL_INFO_ID', 'SPUD', 'RELEASE'])
df_pason_well_info_api = df_pason_well_info_view['API']
df_pason_well_info_api = df_pason_well_info_api[df_pason_well_info_api.notnull()]

phase = input("Intermediate: int, Curve & Lateral: cl")

if phase == 'int':
    # Create an empty list to hold API numbers that will be queried in WellView
    int_api_list = df_pason_well_info_api.values.tolist()

    # Creating a dynamic string for later insertion into SQL query
    int_api_list_string = "\'" + '\' OR \"wellida\" = \''.join(int_api_list) + "\'"

    # Creating list to hold data imported from Snowflake wellview tables
    wellview_well_header = []

    # Create a cursor object used to hold data from Snowflake
    cur2 = ctx.cursor()

    # Querying Snowflake for wellview data...primarily looking for start and end datetimes...joining wellview tables to get there
    try:
        # cur.execute("SELECT wellname, wellida, idwell FROM WELLVIEW.WELLHEADER WHERE wellida = {} ORDER BY wellname".format(int_api_list_string))
        cur2.execute("SELECT wh.\"wellname\", wh.\"wellida\", wh.\"idwell\", job.\"jobtyp\", job.\"idrec\", phs.\"code1\", phs.\"code2\", phs.\"code3\", phs.\"dttmstartactual\", phs.\"dttmendactual\" FROM \"DRILLING_MART\".\"WELLVIEW\".\"WELLHEADER\" AS wh "
        "INNER JOIN \"DRILLING_MART\".\"WELLVIEW\".\"JOB\" AS job ON wh.\"idwell\" = job.\"idwell\" "
        "INNER JOIN \"DRILLING_MART\".\"WELLVIEW\".\"JOB_PROGRAM_PHASE\" AS phs ON job.\"idrec\" = phs.\"idrecparent\" "
        "WHERE ( \"wellida\" = {} ) AND job.\"jobtyp\"=\'DRILLING - ORIGINAL\' AND phs.\"code1\" = \'INTERMEDIATE\' AND phs.\"code2\" = \'DRILL\' AND phs.\"code3\" = \'DRILL\'".format(int_api_list_string)
                     )
        for (wellname, wellida, idwell, jobtyp, idrec, code1, code2, code3, dttmstartactual, dttmendactual) in cur2:
            wellview_well_header.append((wellname, wellida, idwell, jobtyp, idrec, code1, code2, code3, dttmstartactual, dttmendactual))

    # Closing connection to Snowflake
    finally:
        cur2.close()

    # Converting list with Snowflake wellview data into Pandas DataFrame
    df_int_wellview_well_header = pd.DataFrame(wellview_well_header, columns=['WELL_NAME', 'API', 'WELLVIEW_IDWELL', 'JOB_TYP', 'IDREC', 'HOLE_SECTION', 'INTERVAL', 'CATEGORY', 'START_DATE_TIME', 'END_DATE_TIME'])

    # Joining DataFrames of Pason and Wellview of what wells are needed
    df_int_pason_wellview_combined_info = df_pason_well_info_view.join(df_int_wellview_well_header.set_index('API'), on='API', how='right', lsuffix='_PASON', rsuffix='_WV')
    drop_index_numbers = df_int_pason_wellview_combined_info[df_int_pason_wellview_combined_info['START_DATE_TIME'] < (df_int_pason_wellview_combined_info['SPUD'] - timedelta(days=1))].index
    df_int_pason_wellview_combined_info.drop(drop_index_numbers, inplace=True)
    df_int_info_table = df_int_pason_wellview_combined_info[['WELL_NAME_WV', 'WELL_INFO_ID', 'HOLE_SECTION', 'INTERVAL', 'CATEGORY', 'START_DATE_TIME', 'END_DATE_TIME', 'WELL_NAME_PASON', 'SPUD']].drop_duplicates(subset='WELL_INFO_ID', keep='first')

    well_info_id = int(input("Input Pason Well Info ID for desired well"))
    df_int_info_table_filtered = df_int_info_table[df_int_info_table['WELL_INFO_ID'] == well_info_id]
    well_name_wv = str(df_int_info_table_filtered.iloc[0, 0])
    start_date_time = str(df_int_info_table_filtered.iloc[0, 5])
    end_date_time = str(df_int_info_table_filtered.iloc[0, 6])
    today = datetime.today()
    today_string = str(today)
    if end_date_time == 'NaT':
        end_date_time = today_string[0:19]

    pason_1s_sql_string = "(WELL_INFO_ID = " + str(well_info_id) + " AND TIMEPOINT > '" + start_date_time + "' AND TIMEPOINT < '" + end_date_time + "')"

    # Creating list to hold data imported from Snowflake pason 1s traces tables
    pason_time_traces_1s_view = []

    # Query Pason 1s data for footage calculations
    # Create a cursor object used to hold data from Snowflake
    cur3 = ctx.cursor()

    # Querying Snowflake
    try:
        cur3.execute("SELECT WELL_INFO_ID, TIMEPOINT, HOLE_DEPTH, ROP, WT_ON_BIT, ROTARY, RSUP, RSUB "
                    "FROM PASON_TEST.TIME_TRACES_1S_VIEW "
                    "WHERE {} AND HOLE_DEPTH IS NOT NULL "
                    "ORDER BY WELL_INFO_ID, TIMEPOINT".format(pason_1s_sql_string))
        for (WELL_INFO_ID, TIMEPOINT, HOLE_DEPTH, ROP, WT_ON_BIT, ROTARY, RSUP, RSUB) in cur3:
            pason_time_traces_1s_view.append((WELL_INFO_ID, TIMEPOINT, HOLE_DEPTH, ROP, WT_ON_BIT, ROTARY, RSUP, RSUB))

    # Closing connection to Snowflake
    finally:
        cur3.close()

    df_int_pason_time_traces_1s_view = pd.DataFrame(pason_time_traces_1s_view, columns=['WELL_INFO_ID', 'TIMEPOINT', 'HOLE_DEPTH', 'ROP', 'WT_ON_BIT', 'ROTARY', 'RSUP', 'RSUB'])
    df_pason_time_traces_1s_view = pd.DataFrame(pason_time_traces_1s_view, columns=['WELL_INFO_ID', 'TIMEPOINT', 'HOLE_DEPTH', 'ROP', 'WT_ON_BIT', 'ROTARY', 'RSUP', 'RSUB'])
    df_pason_time_traces_1s_view = df_pason_time_traces_1s_view.astype({"HOLE_DEPTH": 'float64', "ROP": 'float64', "WT_ON_BIT": 'float64', "ROTARY": 'float64', "RSUP": 'float64', "RSUB": 'float64'})
    df_pason_on_bottom = df_pason_time_traces_1s_view[(df_pason_time_traces_1s_view['RSUP'] == 1) & (df_pason_time_traces_1s_view['RSUB'] == 1)]
    #df_pason_on_bottom = df_pason_time_traces_1s_view[(df_pason_time_traces_1s_view['RSUP'] == 1) & ((df_pason_time_traces_1s_view['RSUB'] == 1) | (df_pason_time_traces_1s_view['RSUB'] == 2))]
    # & (df_pason_time_traces_1s_view['RSUB'] == 2)
    df_pason_on_bottom = df_pason_on_bottom[['HOLE_DEPTH', 'ROP', 'WT_ON_BIT', 'ROTARY', 'RSUP', 'RSUB']].copy()
    df_pason_on_bottom.set_index('HOLE_DEPTH', inplace=True, drop=True)

    avg_rop = average_trace('ROP')
    avg_rop.set_index('Depth', inplace=True, drop=True)
    avg_weight = average_trace('WT_ON_BIT')
    avg_weight.set_index('Depth', inplace=True, drop=True)
    avg_rotary = average_trace('ROTARY')
    avg_rotary.set_index('Depth', inplace=True, drop=True)
    avg_traces = pd.concat([avg_weight, avg_rotary, avg_rop], axis=1, join='inner')
    avg_traces['Well Name'] = well_name_wv
    #avg_traces.to_csv('C:/Users/dakota.murdock/Desktop/AVGTracesIntermediate.csv', mode='a', header=False)
    df_int_pason_time_traces_1s_view['DEPTH_PROG_24'] = df_int_pason_time_traces_1s_view['HOLE_DEPTH'].diff(periods=86400)
    depth_progress_series_24 = df_int_pason_time_traces_1s_view['DEPTH_PROG_24']
    depth_progress_series_24 = pd.to_numeric(depth_progress_series_24)
    max_int_footage_24 = depth_progress_series_24.max()
    max_int_occurs_24_end = df_int_pason_time_traces_1s_view.loc[depth_progress_series_24.idxmax(), 'HOLE_DEPTH']
    max_int_occurs_24_start = df_int_pason_time_traces_1s_view.loc[depth_progress_series_24.idxmax() - 86400, "HOLE_DEPTH"]

elif phase == 'cl':

    #Create an empty list to hold API numbers that will be queried in WellView
    #df_pason_well_info_api = df_pason_well_info_api.iloc[0:58]
    cl_api_list = df_pason_well_info_api.values.tolist()

    #Creating a dynamic string for later insertion into SQL query
    cl_api_list_string = "\'" + "\' OR \"wellida\" = \'".join(cl_api_list) + "\'"

    #Creating list to hold data imported from Snowflake wellview tables
    cl_wellview_well_header = []

    #Create a cursor object used to hold data from Snowflake
    cur2 = ctx.cursor()

    #Querying Snowflake for wellview data...primarily looking for start and end datetimes...joining wellview tables to get there
    try:
        #cur.execute("SELECT wellname, wellida, idwell FROM WELLVIEW.WELLHEADER WHERE wellida = {} ORDER BY wellname".format(int_api_list_string))
        cur2.execute("SELECT wh.\"wellname\", wh.\"wellida\", wh.\"idwell\", job.\"jobtyp\", job.\"idrec\", phs.\"code1\", phs.\"code2\", phs.\"code3\", phs.\"dttmstartactual\", phs.\"dttmendactual\" FROM \"DRILLING_MART\".\"WELLVIEW\".\"WELLHEADER\" AS wh "
        "INNER JOIN \"DRILLING_MART\".\"WELLVIEW\".\"JOB\" AS job ON wh.\"idwell\" = job.\"idwell\" "
        "INNER JOIN \"DRILLING_MART\".\"WELLVIEW\".\"JOB_PROGRAM_PHASE\" AS phs ON job.\"idrec\" = phs.\"idrecparent\" "
        "WHERE ( \"wellida\" = {} ) AND job.\"jobtyp\"=\'DRILLING - ORIGINAL\' AND (phs.\"code1\" = \'CURVE\' OR phs.\"code1\" = \'LATERAL\') AND phs.\"code2\" = \'DRILL\' AND phs.\"code3\" = \'DRILL\'".format(cl_api_list_string)
                     )
        for (wellname, wellida, idwell, jobtyp, idrec, code1, code2, code3, dttmstartactual, dttmendactual) in cur2:
            cl_wellview_well_header.append((wellname, wellida, idwell, jobtyp, idrec, code1, code2, code3, dttmstartactual, dttmendactual))

    #Closing connection to Snowflake
    finally:
        cur2.close()

    #Converting list with Snowflake wellview data into Pandas DataFrame
    df_cl_wellview_well_header = pd.DataFrame(cl_wellview_well_header, columns=['WELL_NAME', 'API', 'WELLVIEW_IDWELL', 'JOB_TYP', 'IDREC', 'HOLE_SECTION', 'INTERVAL', 'CATEGORY', 'START_DATE_TIME', 'END_DATE_TIME'])

    #Joining DataFrames of Pason and Wellview of what wells are needed
    df_cl_pason_wellview_combined_info = df_pason_well_info_view.join(df_cl_wellview_well_header.set_index('API'), on='API', how='right', lsuffix='_PASON', rsuffix='_WV')
    drop_index_numbers = df_cl_pason_wellview_combined_info[df_cl_pason_wellview_combined_info['START_DATE_TIME'] < (df_cl_pason_wellview_combined_info['SPUD'] - timedelta(days=1))].index
    df_cl_pason_wellview_combined_info.drop(drop_index_numbers, inplace=True)
    df_cl_info_table = df_cl_pason_wellview_combined_info[['WELL_NAME_WV', 'WELL_INFO_ID', 'HOLE_SECTION', 'INTERVAL', 'CATEGORY', 'START_DATE_TIME', 'END_DATE_TIME', 'WELL_NAME_PASON', 'SPUD']]
    well_info_id = int(input("Input Pason Well Info ID for desired well"))
    df_cl_info_table_filtered = df_cl_info_table[df_cl_info_table['WELL_INFO_ID'] == well_info_id].sort_values(by=['HOLE_SECTION'])
    well_name_wv = str(df_cl_info_table_filtered.iloc[0, 0])
    start_date_time = str(df_cl_info_table_filtered.iloc[0, 5])
    #start_date_time = "2020-11-18 22:30:00"
    end_date_time = str(df_cl_info_table_filtered.iloc[1, 6])
    today = datetime.today()
    today_string = str(today)
    if end_date_time == 'NaT':
        end_date_time = today_string[0:19]

    pason_1s_sql_string = "(WELL_INFO_ID = " + str(
        well_info_id) + " AND TIMEPOINT > '" + start_date_time + "' AND TIMEPOINT < '" + end_date_time + "')"

    # Creating list to hold data imported from Snowflake pason 1s traces tables
    pason_time_traces_1s_view = []

    # Query Pason 1s data for footage calculations
    # Create a cursor object used to hold data from Snowflake
    cur3 = ctx.cursor()

    # Querying Snowflake
    try:
        cur3.execute("SELECT WELL_INFO_ID, TIMEPOINT, HOLE_DEPTH, ROP, DIFF_PRESS, PRESSURE, PUMP_RATE, CTOR, WT_ON_BIT, ROTARY, RSUP, RSUB "
                     "FROM PASON_TEST.TIME_TRACES_1S_VIEW "
                     "WHERE {} AND HOLE_DEPTH IS NOT NULL "
                     "ORDER BY WELL_INFO_ID, TIMEPOINT".format(pason_1s_sql_string))
        for (WELL_INFO_ID, TIMEPOINT, HOLE_DEPTH, ROP, DIFF_PRESS, PRESSURE, PUMP_RATE, CTOR, WT_ON_BIT, ROTARY, RSUP, RSUB) in cur3:
            pason_time_traces_1s_view.append((WELL_INFO_ID, TIMEPOINT, HOLE_DEPTH, ROP, DIFF_PRESS, PRESSURE, PUMP_RATE, CTOR, WT_ON_BIT, ROTARY, RSUP, RSUB))

    # Closing connection to Snowflake
    finally:
        cur3.close()

    df_cl_pason_time_traces_1s_view = pd.DataFrame(pason_time_traces_1s_view, columns=['WELL_INFO_ID', 'TIMEPOINT', 'HOLE_DEPTH', 'ROP', 'DIFF_PRESS', 'PRESSURE', 'PUMP_RATE', 'CTOR', 'WT_ON_BIT', 'ROTARY', 'RSUP', 'RSUB'])
    df_cl_pason_time_traces_1s_view = df_cl_pason_time_traces_1s_view.astype({"HOLE_DEPTH": 'float64', "ROP": 'float64', "DIFF_PRESS": 'float64', "PRESSURE": 'float64', "PUMP_RATE": 'float64', "CTOR": 'float64', "WT_ON_BIT": 'float64', "ROTARY": 'float64', "RSUP": 'float64', "RSUB": 'float64'})
    df_cl_pason_on_bottom = df_cl_pason_time_traces_1s_view[(df_cl_pason_time_traces_1s_view['RSUP'] == 1) & (df_cl_pason_time_traces_1s_view['RSUB'] == 1) & (df_cl_pason_time_traces_1s_view['DIFF_PRESS'] > 100)]
    df_pason_on_bottom = df_cl_pason_on_bottom[['HOLE_DEPTH', 'ROP', 'WT_ON_BIT', 'DIFF_PRESS', 'PRESSURE', 'PUMP_RATE', 'CTOR', 'ROTARY', 'RSUP', 'RSUB']].copy()
    df_pason_on_bottom.set_index('HOLE_DEPTH', inplace=True, drop=True)

    avg_rop = average_trace('ROP')
    avg_rop.set_index('Depth', inplace = True, drop=True)
    avg_weight = average_trace('WT_ON_BIT')
    avg_weight.set_index('Depth', inplace = True, drop=True)
    avg_rotary = average_trace('ROTARY')
    avg_rotary.set_index('Depth', inplace = True, drop=True)
    avg_diff = average_trace('DIFF_PRESS')
    avg_diff.set_index('Depth', inplace = True, drop=True)
    avg_pressure = average_trace('PRESSURE')
    avg_pressure.set_index('Depth', inplace=True, drop=True)
    avg_pump_rate = average_trace('PUMP_RATE')
    avg_pump_rate.set_index('Depth', inplace=True, drop=True)
    avg_torque = average_trace('CTOR')
    avg_torque.set_index('Depth', inplace=True, drop=True)
    avg_traces = pd.concat([avg_weight, avg_rotary, avg_diff, avg_pressure, avg_pump_rate, avg_torque, avg_rop], axis=1, join='inner')
    avg_traces['Time'] = 90 / avg_traces['ROP']
    avg_traces['Time'] = avg_traces['Time'].cumsum()
    avg_traces['Well Name'] = well_name_wv
    #avg_traces.to_csv('C:/Users/dakota.murdock/Desktop/AVGTracesProduction.csv', mode='a', header=False)
    df_cl_pason_time_traces_1s_view['DEPTH_PROG_24'] = pd.to_numeric(df_cl_pason_time_traces_1s_view['HOLE_DEPTH']).diff(periods=86400)
    depth_progress_series_24 = df_cl_pason_time_traces_1s_view['DEPTH_PROG_24']
    depth_progress_series_24 = pd.to_numeric(depth_progress_series_24)
    max_cl_footage_24 = depth_progress_series_24.max()
    max_cl_occurs_24_end = df_cl_pason_time_traces_1s_view.loc[depth_progress_series_24.idxmax(), 'HOLE_DEPTH']
    max_cl_occurs_24_start = df_cl_pason_time_traces_1s_view.loc[depth_progress_series_24.idxmax() - 86400, "HOLE_DEPTH"]
    next_cl_occurs_24_start = max_cl_occurs_24_end

    try:
        next_cl_occurs_24_end = df_cl_pason_time_traces_1s_view.loc[depth_progress_series_24.idxmax() + 86400, 'HOLE_DEPTH']
    except IndexError:
        next_cl_occurs_24_end = df_cl_pason_time_traces_1s_view.loc[depth_progress_series_24.index[-1], 'HOLE_DEPTH']
    next_cl_footage_24 = next_cl_occurs_24_end - next_cl_occurs_24_start
    print(max_cl_footage_24)
    print(next_cl_footage_24)