import datetime
import json
from collections import defaultdict
from subprocess import getoutput
from typing import Any, TypedDict

from frappe.frappeclient import FrappeClient

import conf

TimeSheet = dict[str, Any]


class TimeLog(TypedDict):
	id: str
	start: str
	stop: str
	project: str
	tags: list[str]


def main(client: FrappeClient) -> None:

	yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

	time_logs = json.loads(
		getoutput(f"watson log --from {yesterday} --to {yesterday} --json")
	)
	timesheets = get_timesheets(time_logs)
	post_timesheets(timesheets, client=client)


def get_timesheets(time_logs: list[TimeLog]) -> list[TimeSheet]:
	"""Convert Watson timelogs into timesheets.

	group timelogs by project and create timesheets
	by mapping project and tags to customer and tasks."""

	timesheet_map = defaultdict(list)

	for log in time_logs:
		project_name = conf.project_map.get(log["project"]) or log["project"]
		activity = {
			"activity_type": conf.activity_map.get(log["tags"][0]) or log["tags"][0],
			"from_time": _convert_timestamp(log["start"]),
			"hours": _compute_hours(log["stop"], log["start"]),
			"completed": 1,
			"is_billable": 1,
			"project": project_name,
			"description": f"Watson task id: {log['id']}",
		}
		timesheet_map[project_name].append(activity)

	timesheets = []
	for project, activities in timesheet_map.items():
		timesheets.append(
			{
				"doctype": "Timesheet",
				"employee": conf.employee,
				"parent_project": project,
				"start_date": _get_start_date(activities),
				"time_logs": activities,
			}
		)
	return timesheets


def post_timesheets(timesheets: list[TimeSheet], client: FrappeClient) -> None:
	for ts in timesheets:
		filters = [
			["parent_project", "=", ts["parent_project"]],
			["start_date", "=", ts["start_date"]],
		]
		if not client.get_list("Timesheet", filters=filters):
			client.insert(ts)


def _get_start_date(activities) -> str:
	start_date = datetime.datetime.fromisoformat(activities[0]["from_time"])
	return start_date.strftime("%Y-%m-%d")


def _convert_timestamp(iso_ts: str) -> str:
	start_date = datetime.datetime.fromisoformat(iso_ts)
	return start_date.strftime("%Y-%m-%d %H:%M:%S.%f")


def _compute_hours(stop: str, start: str) -> float:
	stop = datetime.datetime.fromisoformat(stop)
	start = datetime.datetime.fromisoformat(start)
	return (stop - start).total_seconds() / (60 * 60)


if __name__ == "__main__":
	client = FrappeClient(conf.base_url, api_key=conf.api_key, api_secret=conf.api_secret)
	exit(main(client=client))
