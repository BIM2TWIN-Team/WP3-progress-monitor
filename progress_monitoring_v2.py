# -*- coding: utf-8 -*-`
# Copyright University of Cambridge 2023. All Rights Reserved.
# Author: Alwyn Mathew <am3156@cam.ac.uk>
# This file cannot be used without a written permission from the author(s).

import argparse
from datetime import datetime

from DTP_API.DTP_API import DTPApi
from DTP_API.DTP_config import DTPConfig
from DTP_API.helpers import convert_str_dtp_format_datetime


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
    activity_progress: str, obligatory
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
        return max(num_days_list)
    else:  # no actions for operation
        return each_activity_tracker['days']


def calculate_projection(activity_tracker, activity_iri):
    """
    Calculate projected finishing days for a delayed operation

    if operation started
    projected day = (number of tasks completed in the activity / number of days taken to complete tasks in the activity)
                    x (number of planned days + number of delayed days)
    else
    projected day = number of planned days + number of delayed days
    """
    # TODO: Projection function should be changed to S-shaped function
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


def get_as_pref_iri_from_as_planned(as_planned_iri):
    """
    Get as-built iri from as-designed iri
    Parameters
    ----------
    as_planned_iri: str
        As-designed iri
    Returns
    -------
    str
        As-built iri
    """
    # TODO: Currently the graph has wrong as-built iri. Once its fixed this function need to be corrected accordingly
    return as_planned_iri.replace('ifc', 'as_builtifc') + '_1'


def compute_progress(activity_tracker, activity_iri, progress_at_activity):
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
    computed_complete = sum(activity_tracker[activity_iri]['complete']) / num_task * 100
    computed_status = activity_status(activity_tracker[activity_iri]['status'])
    computed_num_days = get_num_days(activity_tracker[activity_iri], computed_status)

    progress_at_activity[activity_iri] = {'complete': computed_complete,
                                          'status': computed_status,
                                          'days': computed_num_days}

    # only project dates for delayed operation that are not completed
    if computed_status == "behind" and computed_complete != 100:
        projected_days = calculate_projection(activity_tracker, activity_iri)
        progress_at_activity[activity_iri]['projection'] = projected_days


class ProgressMonitor:

    def __init__(self, dtp_config, dtp_api):
        """
        Parameters
        ----------
        dtp_config : DTP_Config, obligatory
            an instance of DTP_Config
        dtp_api : DTP_Api, obligatory
            an instance of DTP_Api
        """
        self.DTP_CONFIG = dtp_config
        self.DTP_API = dtp_api
        self.progress_at_activity = dict()

    def get_time(self, node):
        """
        Get start and end time of a node

        Parameters
        ----------
        node: dict
            directory with node information

        Returns
        -------
        tuple
            Start and end date of the node
        """
        start_time = node[self.DTP_CONFIG.get_ontology_uri('plannedStart')]
        end_time = node[self.DTP_CONFIG.get_ontology_uri('plannedEnd')]
        return datetime.fromisoformat(start_time), datetime.fromisoformat(end_time)

    def get_progress_from_as_performed_node(self, node):
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
        if self.DTP_CONFIG.get_ontology_uri('progress') in node:
            return node[self.DTP_CONFIG.get_ontology_uri('progress')]
        else:  # no progress recorded
            return 0

    def get_last_scan_date(self, sub_graph):
        """
        Get last scan date

        Parameters
        ----------
        sub_graph: dict
            subgraph fetched

        Returns
        -------
        datetime
            Returns the latest scan date
        """
        last_scan_date = datetime(1, 1, 1)
        for nodes in sub_graph:
            if nodes['asPerformed']:
                for as_built in nodes['asPerformed']:
                    as_perf_date = convert_str_dtp_format_datetime(
                        as_built[self.DTP_CONFIG.get_ontology_uri('timeStamp')])
                    last_scan_date = max(as_perf_date, last_scan_date)
        return last_scan_date

    def get_op_date(self, as_perf_nodes, activity_start_date):
        """
        Get operation date

        Parameters
        ----------
        as_perf_nodes: list
            as-built nodes
        activity_start_date: datetime
            activity start date
        Returns
        -------
        datetime
            Returns the operation date
        """
        last_scan_date = activity_start_date
        for as_perf_node in as_perf_nodes:
            as_perf_date = as_perf_node[self.DTP_CONFIG.get_ontology_uri('timeStamp')]
            last_scan_date = max(convert_str_dtp_format_datetime(as_perf_date), last_scan_date)
        return last_scan_date

    def compute_progress_at_activity(self):
        """
        Compute progress at activity level

        Returns
        -------
        dict
            Dictionary contains the percentage of tasks finished, ahead/behind/on schedule and how many days for each
            activity
        """
        activity_tracker = dict()
        progress_at_activity = dict()
        sub_graph = self.DTP_API.fetch_subgraph()["value"]
        latest_scan_date = self.get_last_scan_date(sub_graph)
        assert latest_scan_date != datetime(1, 1, 1), "No scan date found!"

        for activity_set in sub_graph:
            activity, as_planned, as_perf = activity_set['act'], activity_set['elements'], activity_set['asPerformed']
            as_perf_iris = [each_perf['_iri'] for each_perf in as_perf]
            activity_iri = activity['_iri']
            activity_tracker[activity_iri] = {'complete': [], 'status': [], 'days': [], 'planned_days': 0,
                                              'perf_days': 0}

            activity_start_time, activity_end_time = self.get_time(activity)
            planned_days = (activity_end_time - activity_start_time).days
            activity_tracker[activity_iri]['planned_days'] = planned_days

            # activity has no as-built nodes
            if not as_perf_iris:  # no as-built nodes
                activity_tracker[activity_iri]['complete'].append(0)
                if activity_start_time < latest_scan_date:
                    # if operation needed to be started but not started yet
                    activity_tracker[activity_iri]['status'].append('behind')
                    day_diff = (latest_scan_date - activity_start_time).days
                else:
                    activity_tracker[activity_iri]['status'].append('on')
                    day_diff = (activity_start_time - latest_scan_date).days
                activity_tracker[activity_iri]['days'] = day_diff
                compute_progress(activity_tracker, activity_iri, progress_at_activity)
                continue

            operation_end_time, operation_start_time = self.get_op_date(as_perf,
                                                                        activity_start_time), activity_start_time
            perf_days = (operation_end_time - activity_start_time).days
            activity_tracker[activity_iri]['perf_days'] = perf_days

            # no as-built elements
            if not as_perf:
                continue

            for each_as_planned in as_planned:
                as_pref_iri = get_as_pref_iri_from_as_planned(each_as_planned['_iri'])
                # as-planned element doesnt have corresponding as-built element
                if as_pref_iri not in as_perf_iris:
                    continue

                as_performed_status = self.get_progress_from_as_performed_node(as_pref_iri)
                time_status, days, task_complete_flag = check_schedule(activity_start_time, activity_end_time,
                                                                       operation_start_time, operation_end_time,
                                                                       as_performed_status)

                activity_tracker[activity_iri]['complete'].append(task_complete_flag)
                activity_tracker[activity_iri]['days'].append(days)
                activity_tracker[activity_iri]['status'].append(time_status)

            compute_progress(activity_tracker, activity_iri, progress_at_activity)

        return progress_at_activity


def parse_args():
    """
    Get parameters from user
    """
    parser = argparse.ArgumentParser(description='Perform progress monitoring with DTP graph')
    parser.add_argument('--xml_path', '-x', type=str, help='path to config xml file', default='DTP_API/DTP_config.xml')
    parser.add_argument('--simulation', '-s', default=False, action='store_true')

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    dtp_config = DTPConfig(args.xml_path)
    dtp_api = DTPApi(dtp_config, simulation_mode=args.simulation)
    progress_monitor = ProgressMonitor(dtp_config, dtp_api)
    progress_dict = progress_monitor.compute_progress_at_activity()
    for activity_iri, progress in progress_dict.items():
        print(activity_iri, progress)
