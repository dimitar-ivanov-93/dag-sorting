from collections import defaultdict
from typing import List, Tuple, Dict, Union
import argparse, logging
from collections import deque
from tabulate import tabulate

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


def execute_tasks(task_list: List[List[Union[str, int, List[str]]]], cpu_count: int) -> List[Tuple[int, List[str], str]]:
    # Initialize variables
    task_time_remaining = {}
    completed_tasks = set()
    no_group_tasks = [task for task in task_list if task[0][0] == 'no_group']
    no_group_tasks = no_group_tasks[0][1:] if no_group_tasks else []
    task_list = [family for family in task_list if family[0][0] != 'no_group']

    # Add all tasks to the time remaining dictionary
    for family in task_list:
        for task in family[1:]:
            task_time_remaining[task[0]] = task[1]
    for task in no_group_tasks:
        task_time_remaining[task[0]] = task[1]

    # Initialize the execution map
    execution_map = []

    # Process tasks
    minute = 0
    family_index = 0
    while family_index < len(task_list) or task_time_remaining:
        minute += 1
        next_minute_tasks = []
        current_minute_tasks = set()

        # Process tasks in the current family
        if family_index < len(task_list):
            for task in task_list[family_index][1:]:
                if task[0] in completed_tasks:
                    continue
                if all(dep in completed_tasks for dep in task[2]) and all(dep not in current_minute_tasks for dep in task[2]):
                    next_minute_tasks.append(task[0])
                    current_minute_tasks.add(task[0])
                    task_time_remaining[task[0]] -= 1

                    # If the task is completed, add it to the completed tasks set
                    if task_time_remaining[task[0]] == 0:
                        completed_tasks.add(task[0])
                        del task_time_remaining[task[0]]

                    # If we have reached the CPU limit, stop adding tasks
                    if len(next_minute_tasks) == cpu_count:
                        break

            # If all tasks in the current family are completed, move to the next family
            if all(task[0] in completed_tasks for task in task_list[family_index][1:]):
                family_index += 1

        # Check if we can add a task from the 'no_group' family
        if len(next_minute_tasks) < cpu_count and no_group_tasks:
            task = no_group_tasks[0]
            if all(dep in completed_tasks for dep in task[2]) and all(dep not in current_minute_tasks for dep in task[2]):
                next_minute_tasks.append(task[0])
                current_minute_tasks.add(task[0])
                task_time_remaining[task[0]] -= 1

                # If the task is completed, add it to the completed tasks set
                if task_time_remaining[task[0]] == 0:
                    completed_tasks.add(task[0])
                    del task_time_remaining[task[0]]
                    no_group_tasks.remove(task)

        # Add the current tasks to the execution map
        group_name = task_list[family_index][0][0] if family_index < len(task_list) else 'no_group'
        execution_map.append((minute, next_minute_tasks, group_name))

        # If no tasks were scheduled this minute, break the loop
        if not next_minute_tasks:
            break

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
    execution_map = execute_tasks(reordered_pipeline_list, args.cpu_cores)

    # Calculate the maximum length of the tasks string
    max_tasks_length = max(len(', '.join(tasks)) for _, tasks, _ in execution_map)

    # Calculate the maximum length of the group name string
    max_group_name_length = max(len(group_name) for _, _, group_name in execution_map)

    # Calculate the maximum length for each column
    max_time_length = max(len(str(minute)) for minute, _, _ in execution_map)
    max_tasks_length = max(max_tasks_length, len("Tasks being Executed"))
    max_group_name_length = max(max_group_name_length, len("Group Name"))

    # Add spaces to the headers to match the maximum length of each column
    header_time = "Time" + ' ' * (max_time_length - len("Time"))
    header_tasks = "Tasks being Executed" + ' ' * (max_tasks_length - len("Tasks being Executed"))
    header_group = "Group Name" + ' ' * (max_group_name_length - len("Group Name"))

    print(f"| {header_time} | {header_tasks} | {header_group}")
    print(f"| {'-' * (max_time_length+1)} | {'-' * max_tasks_length} | {'-' * max_group_name_length}")
    for minute, tasks, group_name in execution_map:
        tasks_str = ', '.join(tasks)
        print(f"| {minute:<{(max_time_length+1)}} | {tasks_str:<{max_tasks_length}} | {group_name if group_name != 'no_group' else '':<{max_group_name_length}}")

    print(f"\nMinimum Execution Time = {minute} minutes.")

if __name__ == "__main__":
    main()