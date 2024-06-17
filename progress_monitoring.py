# -*- coding: utf-8 -*-`
# Copyright University of Cambridge 2023. All Rights Reserved.
# Author: Alwyn Mathew <am3156@cam.ac.uk>
# This file cannot be used without a written permission from the author(s).

import argparse
from datetime import datetime

from tqdm import tqdm

from DTP_API.DTP_API import DTPApi
from DTP_API.DTP_config import DTPConfig
from DTP_API.helpers import get_timestamp_dtp_format, convert_str_dtp_format_datetime


def activity_status(time_list):
    """
    Get combined status from a list of task status

    Parameters
    ----------
    time_list: list, obligatory

    Returns
    -------
    str
        Final status from a list of tasks
    """
    status = ['ahead', 'behind', 'on']
    counts = [time_list.count(s) for s in status]
    return status[counts.index(max(counts))]


def get_num_days(each_activity_tracker, computed_status):
    """
    Get number of days an activity is behind/ahead

    Parameters
    ----------
    each_activity_tracker: dict, obligatory
        Dictionary that stores the status and number of days each task is ahead/behind
    computed_status: str, obligatory
        Status of the activity
    Returns
    -------
    int
        Number of days an activity is behind/ahead
    """
    if isinstance(each_activity_tracker['days'], list):  # operation has many actions
        num_days_list = []
        for idx, status in enumerate(each_activity_tracker['status']):
            if status == computed_status:
                num_days_list.append(each_activity_tracker['days'][idx])
        return max(num_days_list) if len(num_days_list) else 0
    else:  # no actions for operation
        return each_activity_tracker['days']


def check_schedule(activity_start_time, activity_end_time, operation_start_time, operation_end_time,
                   activity_progress):
    """
    Deside if the activity is ahead/behind/on schedule

    Parameters
    ----------
    activity_start_time: datetime.datetime, obligatory
        Start time for activity
    activity_end_time: datetime.datetime, obligatory
        End time for activity
    operation_start_time: datetime.datetime, obligatory
        Start time for operation
    operation_end_time: datetime.datetime, obligatory
        End time for operation
    activity_progress: int, obligatory
        Progress of an element in operation

    Returns
    -------
    tuple
        Progress status [ahead, behind, on], number of days ahead/behind (-1 if not started), if completed 1 else 0
    """
    if activity_progress == 100:  # task complete
        if activity_end_time > operation_end_time:
            combined_status = 'ahead', (activity_end_time - operation_end_time).days, 1
        elif activity_end_time < operation_end_time:
            combined_status = 'behind', (operation_end_time - activity_end_time).days, 1
        else:
            combined_status = 'on', 0, 1

    elif activity_progress == 0:  # task not started
        if activity_start_time > operation_start_time:
            combined_status = 'on', -1, 0
        elif activity_start_time < operation_start_time:
            combined_status = 'behind', (operation_end_time - activity_end_time).days, 0
        else:
            combined_status = 'on', -1, 0

    elif activity_progress in [33, 66]:  # progress at 30, 66 percentage (rebar, form work)
        if activity_end_time > operation_end_time:
            combined_status = 'ahead', (activity_end_time - operation_end_time).days, 1
        elif activity_end_time < operation_end_time:
            combined_status = 'behind', (operation_end_time - activity_end_time).days, 1
        else:
            combined_status = 'on', 0, 1

    else:
        raise Exception(f"{activity_progress} cannot be mapped!")

    return combined_status


def calculate_projection(activity_tracker, activity_iri):
    """
    Calculate projected finishing days for a delayed operation

    if operation started
    projected day = (number of tasks completed in the activity / number of days taken to complete tasks in the activity)
                    x (number of planned days + number of delayed days)
    else
    projected day = number of planned days + number of delayed days
    """
    days = activity_tracker[activity_iri]['days']  # number of delay/ ahead days
    days_planned = activity_tracker[activity_iri]['planned_days']  # planned days for an activity
    days_taken = activity_tracker[activity_iri]['perf_days']  # days taken for num_completed
    if days_taken:  # operation started
        num_completed = sum(activity_tracker[activity_iri]['complete'])  # number of tasks completed
        total_days = days_planned + get_num_days(activity_tracker[activity_iri], 'behind')
        projected_days = (num_completed // days_taken) * total_days
    else:  # operation not started
        projected_days = days_planned + days

    return projected_days


class ProgressMonitor:

    def __init__(self, dtp_config, dtp_api, kpi):
        """
        Parameters
        ----------
        dtp_config : DTP_Config, obligatory
            an instance of DTP_Config
        dtp_api : DTP_Api, obligatory
            an instance of DTP_Api
        kpi : obligatory
            a flag to calculate kpis or not
        """
        self.DTP_CONFIG = dtp_config
        self.DTP_API = dtp_api
        self.kpi = kpi
        self.progress_at_activity = dict()

    def __get_progress_from_as_built_node(self, node):
        """
        Get progress of each as-performed node

        Parameters
        ----------
        node: dict,
            JSON mapped to a dictionary. The data contain nodes of the type element.
        Returns
        -------
        str
            The progress of the node
        """
        if self.DTP_CONFIG.get_ontology_uri('progress') in node['items'][0]:
            return node['items'][0][self.DTP_CONFIG.get_ontology_uri('progress')]
        else:  # no progress recorded
            return 0

    def get_time(self, node, as_planned):
        """
        Get start and end time of a node

        Parameters
        ----------
        node: dict
            directory with node information
        as_planned: bool
            if true get time for as-planned node else as-performed
        Returns
        -------
        tuple
            Start and end date of the node
        """
        uri_str = 'planned' if as_planned else 'process'
        start_time = node[self.DTP_CONFIG.get_ontology_uri(uri_str + 'Start')]
        try:
            if self.DTP_CONFIG.get_ontology_uri(uri_str + 'End') in node:
                end_time = node[self.DTP_CONFIG.get_ontology_uri(uri_str + 'End')]
            elif self.DTP_CONFIG.get_ontology_uri(uri_str + 'End') in node \
                    and self.DTP_CONFIG.get_ontology_uri('lastUpdatedOn') in node:
                end_time_op = node[self.DTP_CONFIG.get_ontology_uri(uri_str + 'End')]
                last_update = node[self.DTP_CONFIG.get_ontology_uri('lastUpdatedOn')]
                end_time = get_timestamp_dtp_format(
                    max(convert_str_dtp_format_datetime(end_time_op),
                        convert_str_dtp_format_datetime(last_update)))
            else:
                end_time = node[self.DTP_CONFIG.get_ontology_uri('lastUpdatedOn')]
        except KeyError as err:
            raise Exception(f"{err} for iri: {node['_iri']}")
        return datetime.fromisoformat(start_time), datetime.fromisoformat(end_time)

    def __get_as_performed_op_node(self, as_planned_node):
        """
        Get as-performed operation node from as-planned node

        Parameters
        ----------
        as_planned_node: dict
            Dictionary with node information
        Returns
        -------
        dict
            JSON mapped to a dictionary of an as-performed node.
        """
        return self.DTP_API.fetch_asperformed_connected_asdesigned_oper_nodes(as_planned_node['_iri'])

    def __get_as_performed_element(self, as_planned_node):
        """
        Get as-performed element node from as-planned node

        Parameters
        ----------
        as_planned_node: dict
            Dictionary with node information
        Returns
        -------
        dict
            JSON mapped to a dictionary of an as-performed node.
        """
        return self.DTP_API.fetch_asperformed_connected_asdesigned_nodes(as_planned_node['items'][0]['_iri'])

    def __get_scan_date(self):
        """
        Get latest scan date from operation node

        Returns
        -------
        datetime
            Returns the latest scan date
        """
        scan_date = None
        operations = self.DTP_API.query_all_pages(self.DTP_API.fetch_op_nodes)
        assert operations['size'], "No operation nodes found!"
        for operation in operations['items']:
            if self.DTP_CONFIG.get_ontology_uri('lastUpdatedOn') not in operation:
                continue
            last_updated = operation[self.DTP_CONFIG.get_ontology_uri('lastUpdatedOn')]
            if not scan_date:  # if scan date is not set
                scan_date = last_updated
            else:  # get the latest scan date
                scan_date = get_timestamp_dtp_format(max(convert_str_dtp_format_datetime(scan_date),
                                                         convert_str_dtp_format_datetime(last_updated)))
        return datetime.fromisoformat(scan_date)

    def compute_progress(self, activity_tracker, activity_iri, progress_at_activity):
        """
        Compute progress of each activity

        Parameters
        ----------
        activity_tracker: dict
            Store progress info of each activity
        activity_iri: str
            IRI of activity node
        progress_at_activity: dict
            Dictionary to store progress
        """
        num_task = len(activity_tracker[activity_iri]['complete'])
        if num_task:
            computed_complete = sum(activity_tracker[activity_iri]['complete']) / num_task * 100
        else:
            computed_complete = 0
        computed_status = activity_status(activity_tracker[activity_iri]['status'])
        computed_num_days = get_num_days(activity_tracker[activity_iri], computed_status)

        progress_at_activity[activity_iri] = {'complete': computed_complete,
                                              'status': computed_status,
                                              'days': computed_num_days}

        # only project dates for delayed operation that are not completed
        if computed_status == "behind" and computed_complete != 100:
            projected_days = calculate_projection(activity_tracker, activity_iri)
            progress_at_activity[activity_iri]['projection'] = projected_days

    def compute_progress_at_activity(self, activities=None):
        """
        Compute progress at activity level

        Parameters
        ----------
        activities: dict
            Query response of all activity node
        Returns
        -------
        dict
            Dictionary contains the percentage of tasks finished, ahead/behind/on schedule and how many days for each
            activity
        """
        if activities is None:
            print("Started querying all activity nodes from DTP...")
            activities = self.DTP_API.query_all_pages(self.DTP_API.fetch_activity_nodes)
            print("Completed fetching all activity nodes from DTP.")
        activity_tracker = dict()
        progress_at_activity = dict()
        self.current_date = latest_scan_date = self.__get_scan_date()

        print("Started progress monitering...")
        for each_activity in tqdm(activities['items']):
            # days: activity end date - operation end date (ahead/behind days)
            # planned_days: operation end date - operation start date
            # planned_days: activity end date - activity start date
            activity_tracker[each_activity['_iri']] = {'complete': [], 'status': [], 'days': [], 'planned_days': 0,
                                                       'perf_days': 0, 'days_kpi': []}
            operation_resp = self.__get_as_performed_op_node(each_activity)
            activity_start_time, activity_end_time = self.get_time(each_activity, as_planned=True)
            planned_days = (activity_end_time - activity_start_time).days
            activity_tracker[each_activity['_iri']]['activity_start_time'] = activity_start_time
            activity_tracker[each_activity['_iri']]['activity_end_time'] = activity_end_time
            activity_tracker[each_activity['_iri']]['planned_days'] = planned_days

            if not operation_resp['size']:  # if activity node doesn't have an operation node
                activity_tracker[each_activity['_iri']]['complete'].append(0)
                perf_days = (latest_scan_date - activity_start_time).days
                activity_tracker[each_activity['_iri']]['operation_start_time'] = activity_start_time
                activity_tracker[each_activity['_iri']]['operation_end_time'] = latest_scan_date
                activity_tracker[each_activity['_iri']]['perf_days'] = perf_days
                if activity_start_time < latest_scan_date:
                    # if operation needed to be started but not started yet
                    activity_tracker[each_activity['_iri']]['status'].append('behind')
                    day_diff = (latest_scan_date - activity_start_time).days
                else:
                    activity_tracker[each_activity['_iri']]['status'].append('on')
                    day_diff = (activity_start_time - latest_scan_date).days
                activity_tracker[each_activity['_iri']]['days'] = day_diff
                activity_tracker[each_activity['_iri']][
                    'days_kpi'] = day_diff - planned_days if day_diff > planned_days else planned_days - day_diff
                self.compute_progress(activity_tracker, each_activity['_iri'], progress_at_activity)
                continue

            operation = operation_resp['items'][0]
            operation_start_time, operation_end_time = self.get_time(operation, as_planned=False)
            perf_days = (operation_end_time - operation_start_time).days
            activity_tracker[each_activity['_iri']]['operation_start_time'] = operation_start_time
            activity_tracker[each_activity['_iri']]['operation_end_time'] = operation_end_time
            activity_tracker[each_activity['_iri']]['perf_days'] = perf_days

            tasks = self.DTP_API.query_all_pages(self.DTP_API.fetch_activity_connected_task_nodes,
                                                 each_activity['_iri'])
            for each_task in tasks['items']:
                # assume as-built exist when you find action nodes, not checking as-built node
                # as_planned_element = self.DTP_API.fetch_elements_connected_task_nodes(each_task['_iri'])
                # as_performed_element = self.__get_as_performed_element(as_planned_element)
                # if not as_performed_element['size']:  # if as-planned node doesn't have an as-performed node
                #     continue
                # as_performed_status = self.__get_progress_from_as_built_node(as_performed_element)

                # work around as we assume as-built exist when you find action nodes
                as_performed_status = 100
                time_status, days, task_complete_flag = check_schedule(activity_start_time, activity_end_time,
                                                                       operation_start_time, operation_end_time,
                                                                       as_performed_status)

                activity_tracker[each_activity['_iri']]['complete'].append(task_complete_flag)
                activity_tracker[each_activity['_iri']]['days'].append(days)
                activity_tracker[each_activity['_iri']]['days_kpi'].append(days)
                activity_tracker[each_activity['_iri']]['status'].append(time_status)

            self.compute_progress(activity_tracker, each_activity['_iri'], progress_at_activity)

        if self.kpi:
            self.kpi_calculator(activity_tracker)

        return progress_at_activity

    def compute_progress_at_wp(self, activity_tracker=None):
        """
        Compute progress at work package level

        Parameters
        ----------
        activity_tracker: dict
            Dictionary containing progress at activity
        Returns
        -------
        dict
            Dictionary containing progress at work package
        """
        wp_tracker = dict()
        for activity_iri in activity_tracker:
            wp_iri = self.DTP_API.fetch_workpackage_of_activity_node(activity_iri)['items'][0]['_iri']
            if wp_iri not in wp_tracker:
                wp_tracker[wp_iri] = []
            wp_tracker[wp_iri].append(activity_tracker[activity_iri])

        return wp_tracker

    def kpi_calculator(self, activity_tracker):
        kpi1 = dict()  # percentage of delayed days per work package
        kpi2 = dict()  # percentage of delayed activities per work package

        # go up from activity to work package level
        wp_tracker = self.compute_progress_at_wp(activity_tracker)

        print("Calculating KPIs...")
        for wp_iri, activity_list in wp_tracker.items():
            behind_days = 0
            total_days = 0
            behind_activity_list = []
            precondition_wp = True if open('precondition_record.txt', 'r').read().find(f"{wp_iri}") > -1 else False

            for activity in activity_list:

                current_date = self.current_date if self.current_date > activity['operation_end_time'] \
                    else activity['operation_end_time']
                if precondition_wp:
                    current_date = activity['operation_end_time']

                total_days += (current_date - activity['activity_start_time']).days  # total
                # total_days += (activity['activity_end_time'] - activity['activity_start_time']).days  # planned
                behind_days_check = (current_date - activity['activity_end_time']).days
                if behind_days_check > 0:
                    behind_days += behind_days_check
                    behind_activity_list += [1]
                else:
                    behind_activity_list += [0]

            kpi1[wp_iri] = 0 if behind_days == 0 else behind_days / total_days
            kpi2[wp_iri] = sum(behind_activity_list) / len(behind_activity_list)

        print("KPI 1: percentage of delayed days per work package")
        for wp_iri, kpi_value in kpi1.items():
            print(wp_iri.replace(self.DTP_CONFIG.get_domain(), ""), kpi_value)
        print("KPI 2: percentage of delayed activities per work package")
        for wp_iri, kpi_value in kpi2.items():
            print(wp_iri.replace(self.DTP_CONFIG.get_domain(), ""), kpi_value)


def parse_args():
    """
    Get parameters from user
    """
    parser = argparse.ArgumentParser(description='Perform progress monitoring with DTP graph')
    parser.add_argument('--xml_path', '-x', type=str, help='path to config xml file',
                        default='DTP_API_DTC/DTP_config.xml')
    parser.add_argument('--simulation', '-s', default=False, action='store_true')
    parser.add_argument('--kpis', '-k', default=False, action='store_true')

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    dtp_config = DTPConfig(args.xml_path)
    dtp_api = DTPApi(dtp_config, simulation_mode=args.simulation)
    progress_monitor = ProgressMonitor(dtp_config, dtp_api, args.kpis)
    progress_dict = progress_monitor.compute_progress_at_activity()
