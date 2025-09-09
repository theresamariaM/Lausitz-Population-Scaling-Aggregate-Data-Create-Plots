import pandas as pd
import numpy as np
import duckdb


def leftJoinEnterAndLeaveToRoute(path_df_enterEvents, path_df_leaveEvents, path_df_PersonAndRoute):
    # from gpt 
    df_leave = pd.read_csv(path_df_leaveEvents)
    df_enter = pd.read_csv(path_df_enterEvents)
    df_Route = pd.read_csv(path_df_PersonAndRoute)

    duckdb_conn = duckdb.connect()
    # from gpt 
    duckdb_conn.register("df_PersonAndRoute", df_Route)
    # from gpt 
    duckdb_conn.register("df_enterEvents", df_enter) 
    duckdb_conn.register("df_leaveEvents", df_leave) 

    # query from gpt 
    result= duckdb_conn.query("""SELECT 
        m.vehicle_id,
        m.trip_id,
        m.start_link,
        m.end_link,
        m.dep_time,
        m.arr_time,
        m.travel_time_car,
        m.route_link,
        s1.time AS time_link_entered,
        t2.time AS time_link_left,
        m.type,
        m.length,
        m.freespeed
    FROM df_PersonAndRoute m
    LEFT JOIN df_enterEvents s1 
        ON m.route_link = s1.link
        AND m.vehicle_id = s1.vehicle
        AND s1.time BETWEEN m.dep_time AND m.arr_time                            
    LEFT JOIN df_leaveEvents t2 
        ON m.route_link = t2.link
        AND m.vehicle_id = t2.vehicle
        AND t2.time BETWEEN m.dep_time AND m.arr_time;
                        """).to_df()
    duckdb_conn.close()

    return result

def aggregateResults(df_Routes):
    df_Routes['time_on_link'] = df_Routes['time_link_left'] - df_Routes['time_link_entered']
    df_Routes['speed_on_link'] = df_Routes['length'] / df_Routes['time_on_link']
    df_Routes['check_faster_than_freespeed'] = df_Routes['speed_on_link'] > df_Routes['freespeed']
    
    df_Routes2 = df_Routes[df_Routes['check_faster_than_freespeed'] == False].copy()

    df_Routes2['hour_link_entered'] = np.floor(df_Routes2['time_link_entered'] / 3600)
    df_Routes2 = df_Routes2.sort_values(by=['type', 'hour_link_entered'])
    hour_storage = []
    type_storage = []
    speed_storage = []

    for roadType in df_Routes2['type'].unique():
        for hour in df_Routes2[(df_Routes2['type']== roadType)]['hour_link_entered'].unique():
            hour_storage.append(hour)
            type_storage.append(roadType)
            speed_storage.append(np.mean(df_Routes2[(df_Routes2['type']== roadType) & (df_Routes2['hour_link_entered']== hour)]['speed_on_link']))
    res_aggr = pd.DataFrame({'type': type_storage, 'hour': hour_storage, 'avg_speed': speed_storage})
    return res_aggr

def calcAvgSpeedPerHourForRoadTypeSecTerRes(path_enter, path_leave,path_Routes_with_arr_dep, sample_size_string, sample_nr, alpha, stuck_time, global_seed):

    # left join enter and leave events to data
    df_RouteWithAllTimes = leftJoinEnterAndLeaveToRoute(path_enter, path_leave, path_Routes_with_arr_dep)
    # aggregate results by hour and road type, remove any speed on link that is faster than the freespeed
    df_result = aggregateResults(df_RouteWithAllTimes)
    # insert the necessary information to determine the run
    df_result.insert(3, 'sample_size', sample_size_string)
    df_result.insert(4,'sample_nr', sample_nr) 
    df_result.insert(5, 'alpha', alpha)
    df_result.insert(6, 'stuck_time', stuck_time)
    df_result.insert(7,'global_seed', global_seed)

    return df_result


def calcAvgSpeedFor100pct():
    path_enter = "/net/ils/mersini/input/v2024.2/Link_enter_100pct.csv"
    path_leave = "/net/ils/mersini/input/v2024.2/Link_leave_100pct.csv"
    path_Routes_with_arr_dep = "/net/ils/mersini/input/v2024.2/df_routes_dep_and_arr.csv"
    
    res_case1 = calcAvgSpeedPerHourForRoadTypeSecTerRes(path_enter, path_leave, path_Routes_with_arr_dep,"100-pct", sample_nr= 1, alpha = 1.0, stuck_time=30.0, global_seed="default")
    
    return res_case1



path_part1 = "/net/ils/mersini/output/"

# 100 pct
result_100pct = calcAvgSpeedFor100pct()
output_path_100pct = path_part1 + "Avg_Speed_per_RoadTypeAndHour_100pct_from_df_routes.csv"
result_100pct.to_csv(output_path_100pct, index = False)




