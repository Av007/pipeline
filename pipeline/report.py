import io
from matplotlib import pyplot as plt
import matplotlib
import pandas as pd
from .models import MergedHost
import numpy as np
from typing import List, Dict, Any


class Report:
    def index(self) -> Dict[str, str]:
        data: List[Dict[str, Any]] = [{
            'mergedDocument': item.mergedDocument.to_dict(),
            'daysSinceDiscovered': int(item.daysSinceDiscovered),
            'daysSinceLastSeen': int(item.daysSinceLastSeen),
        } for item in MergedHost.objects(count__gt=1)]

        df = pd.DataFrame(data)

        matplotlib.use('Agg')

        pie = self._pie(df)
        histo = self._histo(df)

        return {
            'pie': '<svg' + pie.getvalue().split('<svg')[1],
            'histo': '<svg' + histo.getvalue().split('<svg')[1],
            'table': self._table_html(df['mergedDocument'].to_list()),
        }

    def _table_html(self, df: List[Dict[str, Any]]) -> str:
        df = pd.DataFrame(df)
        df.set_index('hostname', inplace=True)
        df.drop(columns=[
            'processor',
            'accountId',
            'policies',
            'kernel',
            'tags',
            'biosDescription',
            'externalId',
        ], inplace=True)
        df = df.style.set_caption('Deduped Hosts')
        return df.to_html(classes='table custom-striped', index=False)

    def _histo(self, df) -> io.StringIO:
        fig, ax = plt.subplots()

        old_hosts_days = df['daysSinceLastSeen'].dropna()
        new_hosts_days = df['daysSinceDiscovered'].dropna()

        bins = np.linspace(
            min(old_hosts_days.min(), new_hosts_days.min()),
            max(old_hosts_days.max(), new_hosts_days.max()),
            10
        )

        ax.hist(
            old_hosts_days,
            bins=bins,
            alpha=0.7,
            label='Old Hosts',
            color='coral',
            edgecolor='black'
        )

        ax.hist(
            new_hosts_days,
            bins=bins + 0.1,
            alpha=0.7,
            label='New Hosts',
            color='skyblue',
            edgecolor='black'
        )

        ax.set_xlabel('Days Since Last Seen')
        ax.set_ylabel('Number of Hosts')
        ax.set_title('Old vs. Newly Discovered Hosts')
        ax.legend()

        plt.close()

        img = io.StringIO()
        fig.savefig(img, format='svg')

        return img

    def _pie(self, df) -> io.StringIO:
        fig, ax = plt.subplots()
        os_values = [doc['os'] for doc in df['mergedDocument']]
        os_counts = pd.Series(os_values).value_counts()
        ax.pie(os_counts, labels=os_counts.index, autopct='%1.1f%%', colors=['coral', 'skyblue', 'lightgreen',])
        ax.set_title('Distribution of host by operating system')
        plt.close()

        img = io.StringIO()
        fig.savefig(img, format='svg')

        return img
