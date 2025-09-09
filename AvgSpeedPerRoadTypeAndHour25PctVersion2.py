import matsim 
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import gzip
import duckdb

def networkInfoToDf(pathToNetwork):
    input = gzip.open(pathToNetwork, 'r')
    tree = ET.parse(input)
    root = tree.getroot()
    # convert network to data frame 
    ids = []
    length = []
    freespeed = []
    capacity = []
    type_of_link = []

    for links in root.findall('links'):
        for link in links:
            #print(link.tag, link.attrib)
            ids.append(link.attrib['id'])
            length.append(float(link.attrib['length']))
            freespeed.append(float(link.attrib['freespeed']))
            capacity.append(float(link.attrib['capacity']))
            type_counter = 0
            for child in link:
                for attr in child:
                    if (attr.attrib['name'] == "type"):
                        try:
                            type_of_link.append(attr.text)
                            type_counter = 1
                        except:
                            type_of_link.append('NA')
            if(type_counter == 0):
                type_of_link.append('NA')
    network_df = pd.DataFrame({'link_id': ids, 'length': length, 'freespeed': freespeed, 'capacity': capacity, 'type': type_of_link })
    return network_df

def depAndArrivalsToDf(inputPath):
    pathToEvents = inputPath + ".output_events.xml.gz"
    dep_events = matsim.event_reader(pathToEvents, types= "departure,arrival")

    time_dep = []
    id_dep= []
    link_dep = []
    legMode_dep = []

    time_arr = []
    id_arr = []
    link_arr = []
    legMode_arr = []

    for event in dep_events:
        if event['type'] == "departure":
            time_dep.append(event['time'])
            id_dep.append(event['person'])
            link_dep.append(event['link'])
            legMode_dep.append(event['legMode'])

        elif (event['type'] =="arrival"):
            time_arr.append(event['time'])
            id_arr.append(event['person'])
            link_arr.append(event['link'])
            legMode_arr.append(event['legMode'])


    departures = pd.DataFrame({'person': id_dep, 'time_dep': time_dep, 'link_dep': link_dep, 'mode': legMode_dep})
    departures = departures[(departures['person'].str.contains('pt_') == False)]
    departures = departures[(departures['mode'].str.contains('car') == True)]

    arrivals = pd.DataFrame({'person': id_arr, 'time_arr': time_arr, 'link_arr': link_arr, 'mode': legMode_arr})
    arrivals = arrivals[(arrivals['person'].str.contains('pt_') == False)]
    arrivals = arrivals[(arrivals['mode'].str.contains('car') == True)]

    return [departures, arrivals]

def calcDiffDepArr(departures, arrivals):
    dep_time_storage = []
    arr_time_storage = []
    person_storage = []
    dep_link_storage = []
    arr_link_storage = []
    car_trip_id_storage = []
    vehicle_id_storage = []

    unique_person = departures['person'].unique()
    for person in unique_person:
        temp_depLink = np.array(departures[departures['person'] == person]['link_dep'])
        temp_arrLink = np.array(arrivals[arrivals['person'] == person]['link_arr'])
        dep_time = np.array(departures[departures['person'] == person]['time_dep'])
        arr_time = np.array(arrivals[arrivals['person'] == person]['time_arr'])
        mode_arr = np.array(arrivals[arrivals['person'] == person]['mode'])
        mode_dep = np.array(departures[departures['person'] == person]['mode'])
        # only do this when the person has departures
        car_trip_counter = 0
        # only do this for agents that have the same number of departures and arrivals
        if ((len(dep_time) > 0) & (len(arr_time) > 0) & (len(dep_time) == len(arr_time))):
            for element in range(0, len(dep_time),1):
                # the current departure and arival modes must be the same
                if (mode_arr[element] == mode_dep[element]):
                    if(mode_arr[element]== 'car'):
                        car_trip_counter += 1
                        person_storage.append(person)
                        vehicle_id_storage.append(person + "_" + mode_arr[element])
                        car_trip_id_storage.append(person + "_" + str(car_trip_counter))
                        dep_time_storage.append(dep_time[element])
                        arr_time_storage.append(arr_time[element])
                        dep_link_storage.append(temp_depLink[element])
                        arr_link_storage.append(temp_arrLink[element])


    df_dep_arr = pd.DataFrame({'person': person_storage,'vehicle_id': vehicle_id_storage,'car_trip_id': car_trip_id_storage, 
                               'dep_time': dep_time_storage, 'dep_link': dep_link_storage, 'arr_time': arr_time_storage, 'arr_link': arr_link_storage})
    df_dep_arr['travel_time_car'] = df_dep_arr['arr_time'] - df_dep_arr['dep_time']
    return df_dep_arr

def enterAndLeaveEventToDf(inputPath, df_net):
    pathToEvents = inputPath + ".output_events.xml.gz"
    events = matsim.event_reader(pathToEvents, types= "entered link,left link")

    time_entered = []
    vehicle_entered = []
    link_entered = []
    type_entered = []

    time_left = []
    vehicle_left = []
    link_left = []
    type_left = []

    for event in events:
        if (event["type"] == "entered link"):
            time_entered.append(event['time'])
            vehicle_entered.append(event['vehicle'])
            link_entered.append(event['link'])
            type_entered.append(event['type'])

        elif(event['type']=="left link"):
            time_left.append(event['time'])
            vehicle_left.append(event['vehicle'])
            link_left.append(event['link'])
            type_left.append(event['type'])

            
    df_enter = pd.DataFrame({'enter_time' : time_entered, 'link_id': link_entered, 'vehicle_id': vehicle_entered, 'type_of_event': type_entered})
    df_enter_no_pt = df_enter[(df_enter['link_id'].str.contains('pt_') == False)].copy()
    df_enter_no_pt = pd.merge(df_enter_no_pt, df_net[['link_id', 'type']], how='left', on='link_id')
    # reduce to only the required three types
    df_enter_no_pt2 = df_enter_no_pt[(df_enter_no_pt['type'] == 'highway.secondary') | (df_enter_no_pt['type'] == 'highway.residential') | (df_enter_no_pt['type'] == 'highway.tertiary') ].copy()

    df_leave = pd.DataFrame({'leave_time' : time_left, 'link_id': link_left, 'vehicle_id': vehicle_left, 'type_of_event': type_left})
    df_leave_no_pt = df_leave[(df_leave['link_id'].str.contains('pt_') == False )]
    df_leave_no_pt = pd.merge(df_leave_no_pt, df_net[['link_id', 'type']], how='left', on='link_id')
    df_leave_no_pt2 = df_leave_no_pt[(df_leave_no_pt['type'] == 'highway.secondary') | (df_leave_no_pt['type'] == 'highway.residential') | (df_leave_no_pt['type'] == 'highway.tertiary') ].copy()

    # reduce to only the requires three types
    return [df_enter_no_pt2, df_leave_no_pt2]

def extractCarRouteFromPlans(inputPath, df_net):
    pathToPlans = inputPath + ".output_experienced_plans.xml.gz"
    input = gzip.open(pathToPlans, 'r')
    tree = ET.parse(input)
    root = tree.getroot()

    person_id = []
    vehicle_id = []
    route_storage =[]
    car_trip_number_storage = []
    start_link = []
    end_link = []


#iterate over a persons in the experienced plans file 
    for person in root.findall('person'):
        car_trip_counter = 0
        # there is only the selected plan in the experienced plans file, so it is enough to only find all plans which are children of a person
        for plan in person.findall('plan'):
            if(plan.attrib['selected'] != 'yes'):
                continue
            else:     
                # find all legs
                for leg in plan.findall('leg'):
                    # only for car legs
                    if (leg.attrib['mode'] != "car"):
                        continue
                    elif(leg.attrib['mode'] == "car"):
                        car_trip_counter += 1
                        # find the route
                        for route in leg.findall('route'):              
                            # get all links of the route:
                            temp_route = route.text.split()
                            if (len(temp_route) == 0):
                                print("route of length 0 ")
                                continue
                            else: 
                                if((route.attrib['start_link'] == temp_route[0]) & ((route.attrib['end_link'] == temp_route[len(temp_route)-1])) ):
                                    for element in temp_route[1:len(temp_route)-2]:
                                        person_id.append(person.attrib['id'])
                                        car_trip_number_storage.append(person.attrib['id'] + "_" + str(car_trip_counter))
                                        vehicle_id.append(person.attrib['id'] + "_car")
                                        start_link.append(temp_route[0])
                                        end_link.append(temp_route[len(temp_route)-1])
                                        route_storage.append(element)


    df_Person_Link = pd.DataFrame({'person': person_id, 'trip_id':car_trip_number_storage, 'vehicle_id': vehicle_id, 'route_link':route_storage, 'start_link': start_link, 'end_link': end_link})
    df_Person_Link = pd.merge(df_Person_Link, df_net[['link_id', 'type', 'length', 'freespeed']], how='left', left_on='route_link', right_on = 'link_id')
    df_Person_Link2 = df_Person_Link[(df_Person_Link['type'] == 'highway.secondary') | (df_Person_Link['type'] == 'highway.residential') | (df_Person_Link['type'] == 'highway.tertiary') ].copy()
    
    return df_Person_Link2
                                    
def DepAndArrivalsToRoute(df_TripFromPlan, df_travTime):
    duckdb_conn = duckdb.connect() 
    duckdb_conn.register("df_TripFromPlan", df_TripFromPlan) 
    duckdb_conn.register("df_travTime", df_travTime) 

    #     m.trav_time_in_exp_plan_seconds,
    df_result= duckdb_conn.query("""SELECT 
        m.person,
        m.vehicle_id,
        m.trip_id,
        m.start_link,
        m.end_link,
        m.route_link,
        s1.dep_time,
        s1.arr_time,
        s1.travel_time_car,
        m.type,
        m.length,
        m.freespeed                                                
    FROM df_TripFromPlan m
    LEFT JOIN df_travTime s1 
        ON m.trip_id = s1.car_trip_id
        AND m.vehicle_id = s1.Vehicle_id
        AND m.start_link = s1.dep_link
        AND m.end_link = s1.arr_link ;
                        """).to_df()
    duckdb_conn.close()
    return df_result

def aggregateResultByRoadTypeAndHour(result):
    result['time_on_link'] = result['time_link_left'] - result['time_link_entered']
    result['m_per_s'] = result['length'] / result['time_on_link']
    result['hour_link_entered'] = np.floor(result['time_link_entered'] / 3600)
    result = result.sort_values(by=['type', 'hour_link_entered'])
    hour_storage = []
    type_storage = []
    speed_storage = []

    for roadType in result['type'].unique():
        for hour in result[(result['type']== roadType)]['hour_link_entered'].unique():
            hour_storage.append(hour)
            type_storage.append(roadType)
            speed_storage.append(np.mean(result[(result['type']== roadType) & (result['hour_link_entered']== hour)]['m_per_s'])*3.6)
    res_aggr = pd.DataFrame({'type': type_storage, 'hour': hour_storage, 'speed': speed_storage})
    return res_aggr

def leftJoinEnterAndLeaveToRoute(df_enterEvents, df_leaveEvents, df_PersonAndRoute):
    # from gpt 
    duckdb_conn = duckdb.connect()
    # from gpt 
    duckdb_conn.register("df_PersonAndRoute", df_PersonAndRoute) 
    # from gpt 
    duckdb_conn.register("df_enterEvents", df_enterEvents) 
    duckdb_conn.register("df_leaveEvents", df_leaveEvents) 

    # query from gpt 
    result= duckdb_conn.query("""SELECT 
        m.person,
        m.vehicle_id,
        m.trip_id,
        m.start_link,
        m.end_link,
        m.dep_time,
        m.arr_time,
        m.travel_time_car,
        m.route_link,
        s1.enter_time AS time_link_entered,
        t2.leave_time AS time_link_left,
        m.type,
        m.length,
        m.freespeed
    FROM df_PersonAndRoute m
    LEFT JOIN df_enterEvents s1 
        ON m.route_link = s1.link_id
        AND m.vehicle_id = s1.vehicle_id
        AND s1.enter_time BETWEEN m.dep_time AND m.arr_time                            
    LEFT JOIN df_leaveEvents t2 
        ON m.route_link = t2.link_id
        AND m.vehicle_id = t2.vehicle_id
        AND t2.leave_time BETWEEN m.dep_time AND m.arr_time;
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

def calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, pathToCase, sample_size_string, sample_nr, alpha, stuck_time, global_seed):
   
    # extract arrivals and departures for car legs from events
    df_dep_arr = depAndArrivalsToDf(pathToCase)
    # calculate the travel time 
    df_travel_time_car = calcDiffDepArr(df_dep_arr[0], df_dep_arr[1])
    # extract enter and leave events from events and return only events on link types secondary, tertiary and residential
    df_enter_leave = enterAndLeaveEventToDf(pathToCase, df_network)
    # extract routes from experienced plans, only return route links of type secondary, tertiary and residential
    df_RoutesToDf = extractCarRouteFromPlans(pathToCase, df_network)
    # left join departure and arrival times to df_RoutesToDf
    df_RouteWithArrDepTimes = DepAndArrivalsToRoute(df_RoutesToDf, df_travel_time_car)
    # left join enter and leave events to data
    df_RouteWithAllTimes = leftJoinEnterAndLeaveToRoute(df_enter_leave[0], df_enter_leave[1], df_RouteWithArrDepTimes)
    # aggregate results by hour and road type, remove any speed on link that is faster than the freespeed
    df_result = aggregateResults(df_RouteWithAllTimes)
    # insert the necessary information to determine the run
    df_result.insert(3, 'sample_size', sample_size_string)
    df_result.insert(4,'sample_nr', sample_nr) 
    df_result.insert(5, 'alpha', alpha)
    df_result.insert(6, 'stuck_time', stuck_time)
    df_result.insert(7,'global_seed', global_seed)

    return df_result

def calcAvgSpeedFor5pct(df_network, path_part1):
    flowCapF = ["0.05"]
    storCapF =  ["0.05", "0.10574"]

    avg_speed_per_road_type_and_hour_5pct = pd.DataFrame()

    for fCf in flowCapF:
        for sCf in storCapF:
                for sampleNr in range(1,11,1):
                    # calculate adjusted stuck time
                    default_stuck_time = 30.0
                    adjusted_stuck_time = 30.0/float(flowCapF[0])
                    # declare sample size as str "1-pct"
                    sample_size_as_string = str(int(float(fCf)*100)) + "-pct"

                    # declare path based on case 
                    if((fCf == "0.05") & (sCf == "0.05")):
                        alpha = 1.0
                        if (sampleNr == 6):
                            path_case1  = path_part1 + "output-lausitz-5.0-pct-6-fCf_sCF_0.05_gS_4711_3765/lausitz-5.0-pct-6-fCf_sCF_0.05_gS_4711_3765-2"
                        
                        else: 
                            path_case1 = path_part1 + "output-lausitz-5.0-pct-" + str(sampleNr) + "-fCf_sCF_" + sCf + "_gS_4711_3765/lausitz-5.0-pct-"+str(sampleNr)+ "-fCf_sCF_0.05_gS_4711_3765"
                        
                        
                        path_case3 =  path_part1 + "output-lausitz-5-pct-" + str(sampleNr) + "-fCf_sCF_" + sCf + "_gS_4711_sT_" + str(adjusted_stuck_time) + "_3765/lausitz-5-pct-" + str(sampleNr) +"-fCf_sCF_0.05_gS_4711_sT_600.0_3765"

                        # calculate the average speed per road type and hour for the cases
                        res_case1 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case1, sample_size_as_string, sampleNr, alpha, default_stuck_time, "default")
                        res_case3 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case3, sample_size_as_string, sampleNr, alpha, adjusted_stuck_time, "default")

                        avg_speed_per_road_type_and_hour_5pct = pd.concat([avg_speed_per_road_type_and_hour_5pct, res_case1, res_case3], ignore_index= True)
                        
                    else:
                        alpha = 0.75
                        if(sampleNr == 6):
                            path_case2 = path_part1 + "output-lausitz-5.0-pct-6-fCf_0.05_sCF_0.10574_gS_4711_3765/lausitz-5.0-pct-6-fCf_0.05_sCF_0.10574_gS_4711_3765-2"
                        else:
                            path_case2 = path_part1 + "output-lausitz-5.0-pct-" + str(sampleNr) + "-fCf_" + fCf + "_sCF_" + sCf  + "_gS_4711_3765/lausitz-5.0-pct-"+str(sampleNr)+ "-fCf_0.05_sCF_0.10574_gS_4711_3765"
                        
                        path_case4 = path_part1 + "output-lausitz-5-pct-" + str (sampleNr) + "-fCf_" + fCf + "_sCF_" + sCf + "_gS_4711_sT_" + str(adjusted_stuck_time) + "_3765/lausitz-5-pct-" + str(sampleNr) + "-fCf_0.05_sCF_0.10574_gS_4711_sT_600.0_3765"

                        res_case2 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case2, sample_size_as_string, sampleNr, alpha, default_stuck_time, "default")
                        res_case4 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case4, sample_size_as_string, sampleNr, alpha, adjusted_stuck_time, "default")

                        avg_speed_per_road_type_and_hour_5pct = pd.concat([avg_speed_per_road_type_and_hour_5pct, res_case2, res_case4], ignore_index= True)

    return avg_speed_per_road_type_and_hour_5pct
                    
def calcAvgSpeedFor10pct(df_network, path_part1):
    flowCapF = ["0.1"]
    storCapF =  ["0.1", "0.17783"]

    avg_speed_per_road_type_and_hour_10pct = pd.DataFrame()
    for fCf in flowCapF:
        for sCf in storCapF:
                for sampleNr in range(1,11,1):
                    # calculate adjusted stuck time
                    default_stuck_time = 30.0
                    adjusted_stuck_time = 30.0/float(flowCapF[0])
                    # declare sample size as str "1-pct"
                    sample_size_as_string = str(int(float(fCf)*100)) + "-pct"
                    # declare path based on case 
                    if((fCf == "0.1") & (sCf == "0.1")):
                        alpha = 1.0
                        path_case1 = path_part1 + "output-lausitz-10.0-pct-" + str(sampleNr) + "-fCf_sCF_" + sCf + "_gS_4711_3765/lausitz-10.0-pct-" + str(sampleNr) + "-fCf_sCF_0.1_gS_4711_3765"
                        path_case3 = path_part1 + "output-lausitz-10-pct-" + str(sampleNr) + "-fCf_sCF_" + sCf + "_gS_4711_sT_" + str(adjusted_stuck_time) + "_3765/lausitz-10-pct-" + str(sampleNr) + "-fCf_sCF_0.1_gS_4711_sT_300.0_3765"
                        
                        # calculate the average speed per road type and hour for the cases
                        res_case1 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case1, sample_size_as_string, sampleNr, alpha, default_stuck_time, "default")
                        res_case3 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case3, sample_size_as_string, sampleNr, alpha, adjusted_stuck_time, "default")

                        avg_speed_per_road_type_and_hour_10pct = pd.concat([avg_speed_per_road_type_and_hour_10pct, res_case1, res_case3], ignore_index= True)  
                    else:
                        alpha = 0.75
                        path_case2 = path_part1 + "output-lausitz-10.0-pct-"+ str(sampleNr) + "-fCf_" + fCf + "_sCF_" + sCf +"_gS_4711_3765/lausitz-10.0-pct-" + str(sampleNr) + "-fCf_0.1_sCF_0.17783_gS_4711_3765"
                        path_case4 =path_part1 + "output-lausitz-10-pct-" + str(sampleNr) + "-fCf_" + fCf + "_sCF_" + sCf + "_gS_4711_sT_" + str(adjusted_stuck_time) + "_3765/lausitz-10-pct-" + str(sampleNr) + "-fCf_0.1_sCF_0.17783_gS_4711_sT_300.0_3765"

                        res_case2 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case2, sample_size_as_string, sampleNr, alpha, default_stuck_time, "default")
                        res_case4 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case4, sample_size_as_string, sampleNr, alpha, adjusted_stuck_time, "default")

                        avg_speed_per_road_type_and_hour_10pct = pd.concat([avg_speed_per_road_type_and_hour_10pct, res_case2, res_case4], ignore_index= True)
    
    return avg_speed_per_road_type_and_hour_10pct

def calcAvgSpeedFor25pct(df_network, path_part1):
    flowCapF = ["0.25"]
    storCapF =  ["0.25", "0.35355"]

    avg_speed_per_road_type_and_hour_25pct = pd.DataFrame()
    for fCf in flowCapF:
        for sCf in storCapF:
                for sampleNr in range(1,2,1):
                    # calculate adjusted stuck time
                    default_stuck_time = 30.0
                    adjusted_stuck_time = 30.0/float(flowCapF[0])
                    # declare sample size as str "1-pct"
                    sample_size_as_string = str(int(float(fCf)*100)) + "-pct"
                    
                    # declare path based on case 
                    if((fCf == "0.25") & (sCf == "0.25")):
                        alpha = 1.0
                        path_case1 = path_part1 + "output-lausitz-25.0-pct-fCf_sCF_" +sCf + "_gS_4711_3765/lausitz-25.0-pct-fCf_sCF_0.25_gS_4711_3765"
                        path_case3 = path_part1 + "output-lausitz-25-pct-1-fCf_sCF_" + sCf + "_gS_4711_sT_" + str(adjusted_stuck_time) + "_3765/lausitz-25-pct-1-fCf_sCF_0.25_gS_4711_sT_120.0_3765"
                        
                        # calculate the average speed per road type and hour for the cases
                        res_case1 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case1, sample_size_as_string, sampleNr, alpha, default_stuck_time, "default")
                        res_case3 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case3, sample_size_as_string, sampleNr, alpha, adjusted_stuck_time, "default")

                        avg_speed_per_road_type_and_hour_25pct = pd.concat([avg_speed_per_road_type_and_hour_25pct, res_case1, res_case3], ignore_index= True)  

                    else:
                        alpha = 0.75
                        path_case2 = path_part1 + "output-lausitz-25.0-pct-fCf_" + fCf + "_sCF_" + sCf + "_gS_4711_3765/lausitz-25.0-pct-fCf_0.25_sCF_0.35355_gS_4711_3765"
                        path_case4 = path_part1 + "output-lausitz-25-pct-1-fCf_" + fCf + "_sCF_" + sCf + "_gS_4711_sT_" + str(adjusted_stuck_time) + "_3765/lausitz-25-pct-1-fCf_0.25_sCF_0.35355_gS_4711_sT_120.0_3765"

                        res_case2 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case2, sample_size_as_string, sampleNr, alpha, default_stuck_time, "default")
                        res_case4 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case4, sample_size_as_string, sampleNr, alpha, adjusted_stuck_time, "default")

                        avg_speed_per_road_type_and_hour_25pct = pd.concat([avg_speed_per_road_type_and_hour_25pct, res_case2, res_case4], ignore_index= True)
    
    return avg_speed_per_road_type_and_hour_25pct

def calcAvgSpeedFor50pct(df_network, path_part1):
    flowCapF = ["0.5"]
    storCapF =  ["0.5", "0.5946"]

    avg_speed_per_road_type_and_hour_50pct = pd.DataFrame()
    for fCf in flowCapF:
        for sCf in storCapF:
                for sampleNr in range(1,2,1):
                    # calculate adjusted stuck time
                    default_stuck_time = 30.0
                    adjusted_stuck_time = 30.0/float(flowCapF[0])
                    # declare sample size as str "1-pct"
                    sample_size_as_string = str(int(float(fCf)*100)) + "-pct"

                    # declare path based on case 
                    if((fCf == "0.5") & (sCf == "0.5")):
                        alpha = 1.0
                        path_case1 = path_part1 + "output-lausitz-50.0-pct-fCf_sCF_" + sCf + "_gS_4711_3765/lausitz-50.0-pct-fCf_sCF_0.5_gS_4711_3765"
                        path_case3 = path_part1 + "output-lausitz-50-pct-1-fCf_sCF_" + sCf + "_gS_4711_sT_" + str(adjusted_stuck_time) + "_3765/lausitz-50-pct-1-fCf_sCF_0.5_gS_4711_sT_60.0_3765"
                        
                        # calculate the average speed per road type and hour for the cases
                        res_case1 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case1, sample_size_as_string, sampleNr, alpha, default_stuck_time, "default")
                        res_case3 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case3, sample_size_as_string, sampleNr, alpha, adjusted_stuck_time, "default")

                        avg_speed_per_road_type_and_hour_50pct = pd.concat([avg_speed_per_road_type_and_hour_50pct, res_case1, res_case3], ignore_index= True)     
                    else:
                        alpha = 0.75
                        path_case2 = path_part1 + "output-lausitz-50.0-pct-fCf_" + fCf + "_sCF_" + sCf + "_gS_4711_3765/lausitz-50.0-pct-fCf_0.5_sCF_0.5946_gS_4711_3765"
                        path_case4 =path_part1 + "output-lausitz-50-pct-1-fCf_" + fCf + "_sCF_" + sCf + "_gS_4711_sT_" + str(adjusted_stuck_time) + "_3765/lausitz-50-pct-1-fCf_0.5_sCF_0.5946_gS_4711_sT_60.0_3765"
                        
                        res_case2 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case2, sample_size_as_string, sampleNr, alpha, default_stuck_time, "default")
                        res_case4 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case4, sample_size_as_string, sampleNr, alpha, adjusted_stuck_time, "default")

                        avg_speed_per_road_type_and_hour_50pct = pd.concat([avg_speed_per_road_type_and_hour_50pct, res_case2, res_case4], ignore_index= True)

    return avg_speed_per_road_type_and_hour_50pct
                    

def calcAvgSpeedFor100pct(df_network, path_part1):
    path_case1 = path_part1 + "output-lausitz-100.0-pct-fCf_sCF_1.0_gS_4711_3765/lausitz-100.0-pct-fCf_sCF_1.0_gS_4711_3765"
    res_case1 = calcAvgSpeedPerHourForRoadTypeSecTerRes(df_network, path_case1, "100-pct", sample_nr= 1, alpha = 1.0, stuck_time=30.0, global_seed="default")
    
    return res_case1

# call functions to calculate Results for 5 pct to 100 pct for sample 
pathToNetwork = "/net/ils/mersini/input/v2024.2/lausitz-v2024.2-network.xml.gz"
df_network = networkInfoToDf(pathToNetwork)

path_part1 = "/net/ils/mersini/output/"


# 25 pct 
result_25pct = calcAvgSpeedFor25pct(df_network, path_part1)
output_path_25pct = path_part1 + "Avg_Speed_per_RoadTypeAndHour_25pct.csv"
result_25pct.to_csv(output_path_25pct, index = False)




