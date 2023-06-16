# -*- coding: utf-8 -*-`

# Copyright University of Cambridge 2023. All Rights Reserved.
# Author: Alwyn Mathew <am3156@cam.ac.uk>
# This file cannot be used without a written permission from the author(s).

import argparse
import os.path
import shutil
import time

from tqdm import tqdm

from DTP_API.DTP_API import DTPApi
from DTP_API.DTP_config import DTPConfig
from DTP_API.helpers import logger_global, get_timestamp_dtp_format, convert_str_dtp_format_datetime


class CreateAsPerformed:
    """
    The class is creates all as performed nodes except element level according to as-planned nodes

    Attributes
    ----------
    DTP_CONFIG : class
        an instance of DTP_Config
    DTP_API : DTP_Api, obligatory
            an instance of DTP_Api

    Methods
    -------
    get_all_as_planned_nodes()
        None
    create_as_performed_nodes()
        dict, number of action, operation, and construction nodes created
    """

    def __init__(self, dtp_config, dtp_api, force_update=False, log_path=None):
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
        self.force_update = force_update
        self.as_planned_dict = dict()
        self.created_nodes_iri = {'action': set(), 'operation': set(), 'construction': set()}

        if force_update:
            self.tmp_path = os.path.join(log_path, 'log_nodes')
            if os.path.exists(self.tmp_path):
                shutil.rmtree(self.tmp_path)
            os.makedirs(self.tmp_path)

    def __get_all_work_packages(self):
        """
        Get all work package nodes as list of dicts to as_planned_dict
        """
        print("Started querying all work packages from DTP...")
        all_work_package = self.DTP_API.query_all_pages(self.DTP_API.fetch_workpackage_nodes)
        self.as_planned_dict['work_package'] = all_work_package['items']
        print("Finished querying all work packages from DTP.")

    def __get_activities_for_work_packages(self):
        """
        Get all activity nodes for each work package and add to work package dict with 'activity' key
        """
        print("Started querying activities for each work packages from DTP...")
        for each_wp in self.as_planned_dict['work_package']:
            activities = self.DTP_API.query_all_pages(self.DTP_API.fetch_workpackage_connected_activity_nodes,
                                                      each_wp['_iri'])
            each_wp['activity'] = activities['items']
        print("Finished querying activities for each work packages from DTP.")

    def __get_tasks_for_activities(self):
        """
        Get all task node for each activity and add to activity dict with 'task' key
        """
        print("Started querying tasks for each activities from DTP...")
        for each_wp in self.as_planned_dict['work_package']:
            for each_activity in each_wp['activity']:
                tasks = self.DTP_API.query_all_pages(self.DTP_API.fetch_activity_connected_task_nodes,
                                                     each_activity['_iri'])
                each_activity['task'] = tasks['items']
        print("Finished querying tasks for each activities from DTP.")

    def __get_element_for_tasks(self):
        """
        Get all as-planned element nodes for each task and add to task dict with 'elements' key
        """
        print("Started querying element for each tasks from DTP...")
        for each_wp in tqdm(self.as_planned_dict['work_package']):
            for each_activity in each_wp['activity']:
                for each_task in each_activity['task']:
                    # Always each task will have only one element as its target
                    element = self.DTP_API.query_all_pages(self.DTP_API.fetch_elements_connected_task_nodes,
                                                           each_task['_iri'])
                    each_task['element'] = element['items']
        print("Finished querying element for each tasks from DTP.")

    def __get_all_as_planned_nodes(self):
        """
        Get all as-planned nodes in as_planned_dict dictionary in the following format

        as_planned_dict (root dictionary)
        └── work_package (list of work packages)
            └── activity (list of activities for each work package)
                └── task (list of tasks for each activity)
                    └── element (list of elements for each task)
        """
        self.__get_all_work_packages()
        self.__get_activities_for_work_packages()
        self.__get_tasks_for_activities()
        self.__get_element_for_tasks()

    def __need_to_create_node(self, node_type, node_iri, force=False):
        """
        Check if the node needs to be created or not

        Parameters
        ----------
        node_type: str
            Node type
        node_iri: str
            Node iri
        force: bool
            Need to check force update or not

        Returns
        -------
        bool
            return True if node need to be created else false
        """
        assert node_type in self.created_nodes_iri.keys(), f"Wrong node type '{node_type}'"
        if self.force_update and force:
            return False if node_iri in self.created_nodes_iri[node_type] else True

        return False if node_iri in self.created_nodes_iri[node_type] or self.DTP_API.check_if_exist(
            node_iri) else True

    def __create_action(self, task_dict, as_build_element_iri, process_end):
        """
        Create as-performed action node

        Parameters
        ----------
        task_dict
            Mirror task node
        as_build_element_iri
            List of as-built element iri
        process_end
            End date of action

        Returns
        -------
        str
            return iri of the newly created action node
        """
        action_iri = task_dict['_iri'].replace('task', 'action')
        if not self.__need_to_create_node(node_type='action', node_iri=action_iri):
            return action_iri, False

        task_type = task_dict[self.DTP_CONFIG.get_ontology_uri('hasTaskType')]
        contractor = task_dict[self.DTP_CONFIG.get_ontology_uri('constructionContractor')]
        # TODO: Action start date should be same as that of operation node
        process_start = task_dict[self.DTP_CONFIG.get_ontology_uri('plannedStart')]
        create_res = self.DTP_API.create_action_node(task_type, action_iri, task_dict['_iri'], as_build_element_iri,
                                                     contractor, process_start, process_end)

        if create_res:
            return action_iri, True
        else:
            raise Exception(f"Error creating action node {action_iri}")

    def __create_operation(self, activity, list_of_action_iri, process_end):
        """
        Create as-performed operation node

        Parameters
        ----------
        activity:
            mirror activity node of operation
        list_of_action_iri:
            list of action node connected to this operation node
        process_end:
            End date of operation

        Returns
        -------
        str
            return iri of the newly created operation node
        """
        operation_iri = activity['_iri'].replace('activity', 'operation')
        if not self.__need_to_create_node(node_type='operation', node_iri=operation_iri, force=True):
            return operation_iri, False

        # TODO: Process start should be start of the project or end data of last scan, updateDate should be date of
        #  the latest data accusation and processEnd should be the end of operation. For now, processEnd hold the date
        #  of latest data accusation (as updateDate is not in ontology yet).
        task_type = activity[self.DTP_CONFIG.get_ontology_uri('hasTaskType')]
        process_start = activity[self.DTP_CONFIG.get_ontology_uri('plannedStart')]

        if not self.force_update:
            create_res = self.DTP_API.create_operation_node(task_type, operation_iri, activity['_iri'],
                                                            list_of_action_iri,
                                                            process_start, process_end)
        else:
            create_res = self.DTP_API.update_operation_node(operation_iri, list_of_action_iri,
                                                            process_start, process_end, self.tmp_path)

        if create_res:
            return operation_iri, True
        else:
            raise Exception(f"Error creating operation node {operation_iri}")

    def __create_construction(self, work_package, list_of_operation_iri):
        """
        Create as-performed construction node

        Parameters
        ----------
        work_package
            mirror work package node of operation
        list_of_operation_iri
            list of operation node connected to this construction node

        Returns
        -------
        str
            return iri of the newly created construction node
        """
        constr_iri = work_package['_iri'].replace('workpackage', 'construction')
        if not self.__need_to_create_node(node_type='construction', node_iri=constr_iri, force=True):
            return constr_iri, False

        production_method_type = work_package[self.DTP_CONFIG.get_ontology_uri('hasProductionMethodType')]
        if not self.force_update:
            query_res = self.DTP_API.create_construction_node(production_method_type, constr_iri, work_package['_iri'],
                                                              list_of_operation_iri)
        else:
            query_res = self.DTP_API.update_construction_node(constr_iri, list_of_operation_iri, self.tmp_path)

        if query_res:
            return constr_iri, True
        else:
            raise Exception(f"Error creating/updating construction node {constr_iri}")

    def create_as_performed_nodes(self):
        """
        Create as-performed nodes like Action, Operation and Construction

        Returns
        -------
        dict
            return the number of create node at each level
        """
        self.__get_all_as_planned_nodes()
        print("Started creating as-performed nodes in DTP...")
        for each_wp in tqdm(self.as_planned_dict['work_package']):
            concerned_operation_iris = set()
            operation_action_end_time = None
            for each_activity in each_wp['activity']:
                concerned_action_iris = set()
                for each_task in each_activity['task']:
                    # each task will always have only one element as target
                    element_of_task = each_task['element'][0]

                    # get as-built node connected to as-planned node
                    as_perf_node_response = self.DTP_API.fetch_asperformed_connected_asdesigned_nodes(
                        element_of_task['_iri'])

                    if as_perf_node_response['size'] == 0:  # if no as-built element found, continue to next element
                        continue
                    if as_perf_node_response['size'] > 1:  # if as-planned element has more than one as-built
                        error_msg = f"As-Built node : {element_of_task['_iri']} , connected to " \
                                    f"{as_perf_node_response['size']} as-performed nodes!"
                        logger_global.error(error_msg)
                        Exception(error_msg)

                    as_perf_node = as_perf_node_response['items'][0]
                    # if as-built has zero progress
                    if not as_perf_node[self.DTP_CONFIG.get_ontology_uri('progress')]:
                        continue

                    element_end_time = as_perf_node[self.DTP_CONFIG.get_ontology_uri('timeStamp')]
                    # end date for both operation and action will be same
                    if not operation_action_end_time:  # if operation/action end date is not set
                        operation_action_end_time = element_end_time
                    else:  # get the latest end date
                        operation_action_end_time = get_timestamp_dtp_format(
                            max(convert_str_dtp_format_datetime(operation_action_end_time),
                                convert_str_dtp_format_datetime(element_end_time)))

                    # create corresponding action node
                    action_iri, action_created = self.__create_action(each_task, as_perf_node['_iri'],
                                                                      operation_action_end_time)

                    concerned_action_iris.add(action_iri)
                    if action_created:
                        self.created_nodes_iri['action'].add(action_iri)

                # create corresponding operation node
                operation_iri, operation_created = self.__create_operation(each_activity, concerned_action_iris,
                                                                           operation_action_end_time)
                concerned_operation_iris.add(operation_iri)
                if operation_created:
                    self.created_nodes_iri['operation'].add(operation_iri)

            # create corresponding construction node
            construction_iri, construction_created = self.__create_construction(each_wp, concerned_operation_iris)
            if construction_created:
                self.created_nodes_iri['operation'].add(construction_iri)

        print("Finished creating as-performed in DTP.")

        return {'action': len(self.created_nodes_iri['action']),
                'operation': len(self.created_nodes_iri['operation']),
                'construction': len(self.created_nodes_iri['construction'])}


def parse_args():
    """
    Get parameters from user
    """
    parser = argparse.ArgumentParser(description='Create as-performed nodes in DTP graph')
    parser.add_argument('--xml_path', '-x', type=str, help='path to config xml file', default='DTP_API/DTP_config.xml')
    parser.add_argument('--simulation', '-s', default=False, action='store_true')
    parser.add_argument('--force_update_asperf', default=False, action='store_true',
                        help='if set, nodes will be force to update even if its already exist')
    parser.add_argument('--log_dir', type=str, help='path to log dir', required=True)

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    dtp_config = DTPConfig(args.xml_path)
    dtp_api = DTPApi(dtp_config, simulation_mode=args.simulation)

    # force update node
    if args.force_update_asperf:
        assert args.log_dir, "Please set log dir with --log_dir"

    # logger
    if not os.path.exists(args.log_dir):
        os.makedirs(args.log_dir)
    log_path = os.path.join(args.log_dir, f"db_session-{time.strftime('%Y%m%d-%H%M%S')}.log")
    dtp_api.init_logger(log_path)

    as_performed = CreateAsPerformed(dtp_config, dtp_api, args.force_update_asperf, args.log_dir)
    count_created_nodes = as_performed.create_as_performed_nodes()
    print(f"Created {count_created_nodes['construction']} construction, {count_created_nodes['operation']} "
          f"operation and {count_created_nodes['action']} action nodes.")
