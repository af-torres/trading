import schedule
import time
from .jobs import Last8MondaysOpen, Job

JOBS: list[Job] = [
    Last8MondaysOpen('MSFT')
]


def build_schedule():
    for job in JOBS:
        job.build(schedule)


def main():
    build_schedule()

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
