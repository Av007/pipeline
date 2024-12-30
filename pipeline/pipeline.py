from concurrent.futures import ThreadPoolExecutor
import concurrent
from click import secho
from flask import current_app
from .models import Host
from .providers import Qualys, Crowdstrike
from typing import Dict


class Pipeline:
    """
    Pipeline model class
    """
    def process(self, options: Dict[str, bool | str]) -> None:
        """
        A complex data pipeline
        """
        skip_collecting = options.get('skip_collecting', False)
        skip_aggregating = options.get('skip_aggregating', False)

        if skip_collecting and skip_aggregating:
            secho('At least 1 step should be chosen: skip_collecting or skip_aggregating', fg='red')
            return

        if not skip_collecting:
            providers = [
                Qualys(),
                Crowdstrike()
            ]
            result = []

            exit_on_failure = options.get('exit_on_failure', False)
            skip = options.get('skip', 0)
            limit = options.get('limit', 2)
            total = options.get('total', int(current_app.config.get('TOTAL_DATA_RANGE')) or 10)
            secho(f"Total collection items {total}", fg='green')

            app = current_app._get_current_object()
            with app.app_context():
                tpe = ThreadPoolExecutor(max_workers=5)
                future_data = {}

                try:
                    for provider in providers:
                        for start in range(skip, total, limit):
                            future = tpe.submit(provider.fetch, start, limit)
                            future_data[future] = start

                    for future in concurrent.futures.as_completed(future_data):
                        data_item = future_data[future]
                        try:
                            data = future.result()
                            result.append(data)
                        except Exception as exc:
                            current_app.logger.info('%r generated an exception: %s' % (data_item, exc))
                            secho('%r generated an exception: %s' % (data_item, exc))
                            if exit_on_failure:
                                current_app.logger.info("Shutting down due to failure")
                                tpe.shutdown(wait=True, cancel_futures=False)
                                break
                finally:
                    if not exit_on_failure:
                        tpe.shutdown(wait=True, cancel_futures=False)

            secho('Collection step was successfully finished.')

        if not skip_aggregating:
            secho('Aggregation step started.')
            Host.objects.aggregate_by_hostname()
            secho('Aggregation step was successfully finished.')
