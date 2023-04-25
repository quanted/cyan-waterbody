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
		# self.scheduler.add_job(self.aggregation_tasks.scheduled_aggregation, trigger="cron", args=["daily"], minute="*")  # testing job
		self.scheduler.add_job(
			self.aggregation_tasks.scheduled_aggregation,
			trigger="cron",
			args=["daily"],
			hour="*/4",
			minute="30"
		)  # every 4 hours at minute 30
		self.scheduler.add_job(
			self.aggregation_tasks.scheduled_aggregation,
			trigger="cron",
			args=["weekly"],
			hour="*/8",
			minute="0"
		)  # every 8 hours

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