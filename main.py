from collections import defaultdict
from typing import List, Tuple, Dict, Union
import argparse, logging

logging.basicConfig(filename='error.log', level=logging.ERROR, format='%(asctime)s %(levelname)s:%(message)s')

def read_pipeline_file(filename: str) -> List[List[Union[str, int, List[str]]]]:
    try:
        with open(filename, 'r') as file:
            lines = file.readlines()
    except FileNotFoundError:
        logging.error(f"Error: The file {filename} does not exist.")
        return []
    except Exception as e:
        logging.error(f"Error: An unexpected error occurred while reading the file: {e}")
        return []

    pipeline_list = []
    i = 0
    while i < len(lines):
        try:
            if lines[i].strip() == "END":
                break
            task_name = lines[i].strip()
            execution_time = int(lines[i+1].strip())
            group_name = lines[i+2].strip() if lines[i+2].strip() else "no_group"
            dependencies = lines[i+3].strip().split(',') if lines[i+3].strip() else []
        except ValueError:
            logging.error(f"Error: Invalid data format in the pipeline file at line {i+1}.")
            return []
        except Exception as e:
            logging.error(f"Error: An unexpected error occurred while processing the pipeline file: {e}")
            return []

        # Check if the group already exists
        group = next((g for g in pipeline_list if g[0][0] == group_name), None)
        if group is None:
            # If not, create a new group
            group = [[group_name]]
            pipeline_list.append(group)
        
        # Add the task to the group
        group.append([task_name, execution_time, dependencies])

        i += 4

    return pipeline_list

def topological_sort_util(graph: Dict[str, set], node: str, visited: Dict[str, bool], stack: List[str]) -> None:
    visited[node] = True
    for i in graph[node]:
        if visited[i] == False:
            topological_sort_util(graph, i, visited, stack)
    stack.insert(0, node)

def topological_sort(graph: Dict[str, set], nodes: List[str]) -> List[str]:
    visited = {node: False for node in nodes}
    stack = []
    for node in nodes:
        if visited[node] == False:
            topological_sort_util(graph, node, visited, stack)
    return stack

def reorder_tasks(pipeline_list: List[List[Union[str, int, List[str]]]]) -> List[List[Union[str, int, List[str]]]]:
    for group in pipeline_list:
        group_name, tasks = group[0][0], group[1:]
        graph = defaultdict(set)
        nodes = []
        for task in tasks:
            task_name, execution_time, dependencies = task
            nodes.append(task_name)
            for dependency in dependencies:
                graph[dependency].add(task_name)
        sorted_tasks = topological_sort(graph, nodes)
        sorted_tasks_with_info = sorted(tasks, key=lambda x: sorted_tasks.index(x[0]))
        group[1:] = sorted_tasks_with_info
    return pipeline_list


def execution(groups: List[List[Union[str, int, List[str]]]], cpu_power: int) -> List[Tuple[str, List[List[Union[str, int, List[str]]]]]]:
    execution_map = []
    no_group_tasks = [task for group in groups if group[0][0] == 'no_group' for task in group[1:]]
    for group in groups:
        if group[0][0] == 'no_group':
            continue
        tasks = group[1:]
        while tasks:
            current_tasks = []
            for _ in range(cpu_power):
                if tasks and (not current_tasks or tasks[0][2] == [] or set(tasks[0][2]).isdisjoint(set(t[0] for t in current_tasks))):
                    current_task = tasks[0]
                    current_task[1] -= 1
                    if current_task[1] == 0:
                        tasks.pop(0)
                    current_tasks.append(current_task)
                elif no_group_tasks and (not current_tasks or no_group_tasks[0][2] == [] or set(no_group_tasks[0][2]).isdisjoint(set(t[0] for t in current_tasks))):
                    current_task = no_group_tasks[0]
                    current_task[1] -= 1
                    if current_task[1] == 0:
                        no_group_tasks.pop(0)
                    current_tasks.append(current_task)
            execution_map.append((group[0][0], current_tasks))

    # Execute remaining no_group_tasks
    while no_group_tasks:
        current_tasks = []
        for _ in range(cpu_power):
            if no_group_tasks and (not current_tasks or no_group_tasks[0][2] == [] or set(no_group_tasks[0][2]).isdisjoint(set(t[0] for t in current_tasks))):
                current_task = no_group_tasks[0]
                current_task[1] -= 1
                if current_task[1] == 0:
                    no_group_tasks.pop(0)
                current_tasks.append(current_task)
        execution_map.append(('no_group', current_tasks))

    return execution_map


def check_positive(value: str) -> int:
    ivalue = int(value)
    if ivalue < 1:
         raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue


def main() -> None:
    parser = argparse.ArgumentParser(description='This script optimizes a machine learning data pipeline. It takes a pipeline file and the number of CPU cores as inputs. The pipeline file should define the tasks in the pipeline, each with an execution time, a group, and dependencies. The script computes the minimum execution time of the pipeline, considering that at most cpu_cores number of tasks can be executed in parallel. All tasks in a group should be executed before proceeding with another group, except for tasks that do not belong to any group. The script also outputs a scheduling diagram of the optimal solution.')
    parser.add_argument('--cpu_cores', type=check_positive, required=True, help='Number of CPU cores available for executing tasks in parallel. Has to be an integer of 1 or above.')
    parser.add_argument('--pipeline', type=str, required=True, help='Path to the pipeline file. The file should define the tasks in the pipeline, each with an execution time, a group, and dependencies. The file has to be in the same directory as the script.')
    args = parser.parse_args()

    pipeline_list = read_pipeline_file(args.pipeline)
    if not pipeline_list:
        return
    reordered_pipeline_list = reorder_tasks(pipeline_list)
    execution_map = execution(reordered_pipeline_list, args.cpu_cores)

    # Calculate the maximum length of the tasks string
    max_tasks_length = max(len(', '.join(task[0] for task in tasks)) for _, tasks in execution_map)

    print("| Time    | Tasks being Executed | Group Name")
    print("| ------- | {0} | ----------".format('-' * max_tasks_length))
    min_time = 0
    for i, (group_name, tasks) in enumerate(execution_map, start=1):
        min_time += 1
        tasks_str = ', '.join(task[0] for task in tasks)
        print("| {:<6}  | {:<{}} | {:<10}".format(i, tasks_str, max_tasks_length, group_name if group_name != "no_group" else ""))

    print(f"\nMinimum Execution Time = {min_time} minutes.")

if __name__ == "__main__":
    main()