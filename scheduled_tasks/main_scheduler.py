from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from pytz import utc

from scheduled_tasks.aggregation_tasks import AggregationTasks
from scheduled_tasks.report_tasks import ReportTasks



class MainScheduler:

	def __init__(self):
		self.aggregation_tasks = AggregationTasks()
		self.report_tasks = ReportTasks()
		self.scheduler = None
		self.setup_scheduled_tasks()

	def setup_scheduled_tasks(self):
		"""
		Sets up scheduler and tasks.
		"""
		self.scheduler = BackgroundScheduler(daemon=True, timezone=utc)
		self.add_aggregation_tasks()  # adds aggregation tasks to scheduler
		self.add_report_tasks()  # adds report tasks to scheduler
		self.scheduler.start()  # starts scheduler

	def add_aggregation_tasks(self):
		"""
		Aggregation tasks for daily and weekly data.
		"""
		# Test job (runs daily every minute)
		# self.scheduler.add_job(self.aggregation_tasks.scheduled_aggregation, trigger="cron", args=["daily"], minute="*")  # testing job

		# Runs daily aggregation 2x/day:
		self.scheduler.add_job(
			self.aggregation_tasks.scheduled_aggregation,
			trigger="cron",
			args=["daily"],
			hour="9",
			minute="30"
		)  # every day at 5:30am EST (9:30am UTC)
		self.scheduler.add_job(
			self.aggregation_tasks.scheduled_aggregation,
			trigger="cron",
			args=["daily"],
			hour="23",
			minute="30"
		)  # every day at 7:30pm EST (11:30pm UTC)

		# Runs weekly aggregation every evening:
		self.scheduler.add_job(
			self.aggregation_tasks.scheduled_aggregation,
			trigger="cron",
			args=["weekly"],
			hour="22",
			minute="30"
		)  # every evening at 6:30pm EST (10:30pm UTC)
		# self.scheduler.add_job(
		# 	self.aggregation_tasks.scheduled_aggregation,
		# 	trigger="cron",
		# 	args=["weekly"],
		# 	hour="8",
		# 	minute="0"
		# )  # every morning at 4:00am EST (8:00am UTC)

	def add_report_tasks(self):
		"""
		Report tasks for monthly scheduled state and alpine reports.
		"""
		# self.scheduler.add_job(self.report_tasks.execute_scheduled_reports, trigger="cron", minute="*")  # testing job
		self.scheduler.add_job(
			self.report_tasks.execute_scheduled_reports,
			trigger="cron",
			second=0,
			minute=15,
			hour=4,
			day=1,
			month="*",
			year="*",
			day_of_week="*"
		)  # Executes 4:15AM (UTC) on 1st day of every month of every year on any day of the week