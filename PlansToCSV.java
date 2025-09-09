package org.matsim.run.analysis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.matsim.api.core.v01.TransportMode;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.api.core.v01.population.*;
import org.matsim.core.network.NetworkUtils;
import org.matsim.core.population.PopulationUtils;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


final class PlansToCSV {
	private static final Logger log = LogManager.getLogger(PlansToCSV.class);

	private PlansToCSV() {
		// not called
	}

	public static void main(String[] args) {
		String pathToPopulation = "/home/lola/math_cluster/output/output-lausitz-50.0-pct-fCf_sCF_0.5_gS_4711_3765/lausitz-50.0-pct-fCf_sCF_0.5_gS_4711_3765.output_experienced_plans.xml.gz";
		String outputPath = "/home/lola/Nextcloud/Masterarbeit/03_Outputs/lausitz-v2024.2-50.0-pct-plans-case-1.csv";
		Population population = PopulationUtils.readPopulation(pathToPopulation);

		String pathToNetwork = "/home/lola/Downloads/lausitz-v2024.2-network(1).xml.gz";
		Network network = NetworkUtils.readNetwork(pathToNetwork);
		HashMap<String, String> net = networkToMap(network);


		ArrayList<ArrayList<String>> plansToTable = planToTable(population, net);
		try {
			writePlanTableToCSV(plansToTable, outputPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static HashMap<String, String> networkToMap(Network network) {
		HashMap<String, String> networkToDf = new HashMap<>();
		for (Link link : network.getLinks().values()) {
			networkToDf.put(link.getId().toString(), link.getAttributes().getAttribute("type").toString());
		}
		return networkToDf;
	}


	private static ArrayList<ArrayList<String>> planToTable(Population population, HashMap<String, String> network) {
		ArrayList<ArrayList<String>> plansToTable = new ArrayList<>();
		for (Person person : population.getPersons().values()) {
			Plan plan = person.getSelectedPlan();
			if (plan != null) {
				int car_trip_counter = 0;
				for (int i = 0; i < plan.getPlanElements().size(); i++) {
					if (plan.getPlanElements().get(i) instanceof Leg leg) {
						if (TransportMode.car.equals(leg.getMode())) {
							car_trip_counter++;
							Route route = leg.getRoute();
							if (route != null) {
								ArrayList<String> route_links = new ArrayList<String>(Arrays.asList(route.getRouteDescription().split(" ")));
								for (int route_link_n = 1; route_link_n < route_links.size() - 2; route_link_n++) {
									if ((network.get(route_links.get(route_link_n)).equals("highway.secondary")) ||
										(network.get(route_links.get(route_link_n)).equals("highway.tertiary") ||
											(network.get(route_links.get(route_link_n)).equals("highway.residential")))) {
										ArrayList<String> row = new ArrayList<>();
										row.add(person.getId().toString());
										row.add(person.getId().toString() + "_car");
										row.add(person.getId().toString() + "_" + car_trip_counter);
										row.add(route.getStartLinkId().toString());
										row.add(route.getEndLinkId().toString());
										row.add(route_links.get(route_link_n));
										plansToTable.add(row);
									}

								}
							}
						}
					}
				}
			}
		}
		return plansToTable;
	}

	public static void writePlanTableToCSV(ArrayList<ArrayList<String>> planToTable, String outputPath) throws IOException {
		if (planToTable.isEmpty()) return;
		try {
			BufferedWriter file = new BufferedWriter(new FileWriter(outputPath));
			for (int rows = 0; rows < planToTable.size(); rows++) {
				for (int i = 0; i < 6; i++) {
					file.write(planToTable.get(rows).get(i));
					if (i < 5) {
						file.write(",");
					}
				}
				file.write("\n");
			}
			file.close();
		} catch (IOException exep) {
			log.info("could not create csv file");
		}
	}
}


