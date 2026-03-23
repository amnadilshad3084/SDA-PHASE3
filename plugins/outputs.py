import time
import multiprocessing
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from collections import deque


def get_color_and_label(fill_percent: float):
    if fill_percent < 40:
        return '#2ECC71', 'FLOWING'
    elif fill_percent < 75:
        return '#F39C12', 'FILLING'
    else:
        return '#E74C3C', 'BACKPRESSURE'


class LiveDashboard:
    def __init__(self, processed_queue: multiprocessing.Queue,
                 raw_queue: multiprocessing.Queue,
                 verified_queue: multiprocessing.Queue,
                 config: dict):
        self.processed_queue = processed_queue
        self.raw_queue       = raw_queue
        self.verified_queue  = verified_queue
        self.config          = config
        self.time_values     = deque(maxlen=200)
        self.metric_values   = deque(maxlen=200)
        self.average_values  = deque(maxlen=200)
        self.max_size = config['pipeline_dynamics']['stream_queue_max_size']
        charts = config['visualizations']['data_charts']
        self.chart1_title = charts[0]['title'] if len(charts) > 0 else 'Live Values'
        self.chart2_title = charts[1]['title'] if len(charts) > 1 else 'Running Average'

    def run(self):
        plt.style.use('dark_background')
        self.fig = plt.figure(figsize=(8, 5), facecolor='#0D1117')
        self.fig.suptitle('LIVE PIPELINE DASHBOARD',
                          fontsize=16, fontweight='bold', color='#58A6FF', y=0.98)
        gs = self.fig.add_gridspec(3, 3, hspace=1.2, wspace=0.4, top=0.88, bottom=0.08)
        self.ax_raw       = self.fig.add_subplot(gs[0, 0])
        self.ax_verified  = self.fig.add_subplot(gs[0, 1])
        self.ax_processed = self.fig.add_subplot(gs[0, 2])
        self.ax_values    = self.fig.add_subplot(gs[1, :])
        self.ax_average   = self.fig.add_subplot(gs[2, :])

        time.sleep(2)

        while self._drain_queue():
            pass

        self._update_frame(0)
        self.fig.savefig('dashboard_output.png', dpi=100, bbox_inches='tight', facecolor='#0D1117')
        print('dashboard saved as dashboard_output.png')
        plt.close()

    def _drain_queue(self):
        while True:
            try:
                item = self.processed_queue.get_nowait()
                if item is None:
                    return False
                self.time_values.append(item.get('time_period', 0))
                self.metric_values.append(item.get('metric_value', 0))
                self.average_values.append(item.get('computed_metric', 0))
            except Exception:
                break
        return True

    def _draw_telemetry_bar(self, ax, label, fill_percent):
        ax.clear()
        color, status = get_color_and_label(fill_percent)
        ax.barh(0, 100, color='#21262D', height=0.5)
        ax.barh(0, fill_percent, color=color, height=0.5, alpha=0.9)
        ax.set_xlim(0, 100)
        ax.set_ylim(-0.5, 0.5)
        ax.set_title(label, fontsize=9, color='#8B949E', pad=4)
        ax.text(50, 0, f'{status}  {fill_percent:.0f}%',
                ha='center', va='center', fontsize=8, fontweight='bold', color='white')
        ax.axis('off')
        ax.set_facecolor('#0D1117')

    def _update_frame(self, frame):
        self._drain_queue()
        telemetry_cfg = self.config['visualizations']['telemetry']
        if telemetry_cfg.get('show_raw_stream'):
            self._draw_telemetry_bar(self.ax_raw, 'RAW STREAM', 72)
        if telemetry_cfg.get('show_intermediate_stream'):
            self._draw_telemetry_bar(self.ax_verified, 'VERIFIED STREAM', 45)
        if telemetry_cfg.get('show_processed_stream'):
            self._draw_telemetry_bar(self.ax_processed, 'PROCESSED STREAM', 18)

        self.ax_values.clear()
        self.ax_values.set_facecolor('#161B22')
        if self.time_values:
            self.ax_values.plot(list(self.time_values), list(self.metric_values), color='#58A6FF', linewidth=1.5)
            self.ax_values.fill_between(list(self.time_values), list(self.metric_values), alpha=0.15, color='#58A6FF')
        self.ax_values.set_title(self.chart1_title, fontsize=10, color='#E6EDF3', pad=6)
        self.ax_values.set_xlabel('Time Period', fontsize=8, color='#8B949E')
        self.ax_values.set_ylabel('Metric Value', fontsize=8, color='#8B949E')
        self.ax_values.grid(True, alpha=0.2, color='#30363D')
        self.ax_values.ticklabel_format(style='plain', axis='x')

        self.ax_average.clear()
        self.ax_average.set_facecolor('#161B22')
        if self.average_values:
            self.ax_average.plot(list(self.time_values), list(self.average_values), color='#3FB950', linewidth=1.5)
            self.ax_average.fill_between(list(self.time_values), list(self.average_values), alpha=0.15, color='#3FB950')
        self.ax_average.set_title(self.chart2_title, fontsize=10, color='#E6EDF3', pad=6)
        self.ax_average.set_xlabel('Time Period', fontsize=8, color='#8B949E')
        self.ax_average.set_ylabel('Running Average', fontsize=8, color='#8B949E')
        self.ax_average.grid(True, alpha=0.2, color='#30363D')
        self.ax_average.ticklabel_format(style='plain', axis='x')
