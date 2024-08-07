import argparse
from process_handlers.dsp_process_handler import DspProcessHandler
from process_handlers.eds_process_handler import EdsProcessHandler
from process_handlers.eds_high_frequency_process_handler import EdsHighFrequencyProcessHandler
from process_handlers.eds_monthly_process_handler import EdsMonthlyProcessHandler
from process_handlers.dsp_after_dark_process_handler import DspAfterDarkProcessHandler
from process_handlers.dsp_high_frequency_process_handler import DspHighFrequencyProcessHandler
from process_handlers.on_demand_process_handler import OnDemandProcessHandler
from process_handlers.dsp_airtable_process_handler import DspAirtableProcessHandler
from process_handlers.megaphone_process_handler import MegaphoneProcessHandler
from process_handlers.braze_process_handler import BrazeProcessHandler
from process_handlers.dsp_popularity_process_handler import DspPopularityProcessHandler
from process_handlers.quarterly_sales_process_handler import QuarterlySalesProcessHandler
from process_handlers.dsp_supporting_cast_process_handler import DspSupportingCastProcessHandler
from process_handlers.graph_process_handler import GraphProcessHandler
from process_handlers.dsp_youtube_process_handler import DspYouTubeProcessHandler
from process_handlers.monthly_premium_attributions_process_handler import MonthlyPremiumAttributionsProcessHandler
from process_handlers.weekly_data_review_process_handler import WeeklyDataReviewProcessHandler
from process_handlers.yt_channel_scraper_process_handler import YTChannelScraperProcessHandler
from process_handlers.sales_metrics_process_handler import SalesMetricsProcessHandler
from process_handlers.channel_trajectory_process_handler import ChannelTrajectoryProcessHandler
from process_handlers.data_monitoring_process_handler import DataMonitoringProcessHandler


def main():
    parser = argparse.ArgumentParser(
        description="Let's run some ETL!"
    )
    parser.add_argument('-s', '--server',
                        help="eds[''|'-hf' | '-m'] | dsp[''|'-hf'|'-sc'|'-ad'|'-p'|'-yt'|'-at'] | megaphone | graph | quarterly-sales | weekly-data-review | yt-channel-scraper | sales-metrics | mpa | on-demand | braze | channel-trajectory | data-monitoring")
    parser.add_argument('-l', '--local', action='store_const', const=1, help="turns on Local mode")
    args = parser.parse_args()

    if args.server == 'eds':
        eph = EdsProcessHandler(args.local)
        eph.run_jobs()
    elif args.server == 'eds-hf':
        ehfph = EdsHighFrequencyProcessHandler(args.local)
        ehfph.run_jobs()
    elif args.server == 'on-demand':
        odph = OnDemandProcessHandler(args.local)
        odph.run_jobs()
    elif args.server == 'eds-m':
        ehfph = EdsMonthlyProcessHandler(args.local)
        ehfph.run_jobs()
    elif args.server == 'dsp':
        dph = DspProcessHandler(args.local)
        dph.run_jobs()
    elif args.server == 'dsp-hf':
        ehfph = DspHighFrequencyProcessHandler(args.local)
        ehfph.run_jobs()
    elif args.server == 'braze':
        bph = BrazeProcessHandler(args.local)
        bph.run_jobs()
    elif args.server == 'dsp-sc':
        dsph = DspSupportingCastProcessHandler(args.local)
        dsph.run_jobs()
    elif args.server == 'dsp-ad':
        dspad = DspAfterDarkProcessHandler(args.local)
        dspad.run_jobs()
    elif args.server == 'megaphone':
        dmph = MegaphoneProcessHandler(args.local)
        dmph.run_jobs()
    elif args.server == 'dsp-p':
        dpph = DspPopularityProcessHandler(args.local)
        dpph.run_jobs()
    elif args.server == 'dsp-yt':
        dytph = DspYouTubeProcessHandler(args.local)
        dytph.run_jobs()
    elif args.server == 'dsp-at':
        datph = DspAirtableProcessHandler(args.local)
        datph.run_jobs()
    elif args.server == 'quarterly-sales':
        dqstp = QuarterlySalesProcessHandler(args.local)
        dqstp.run_jobs()
    elif args.server == 'graph':
        gph = GraphProcessHandler(args.local)
        gph.run_jobs()
    elif args.server == 'weekly-data-review':
        wdrph = WeeklyDataReviewProcessHandler(args.local)
        wdrph.run_jobs()
    elif args.server == 'yt-channel-scraper':
        ytcsph = YTChannelScraperProcessHandler(args.local)
        ytcsph.run_jobs()
    elif args.server == 'sales-metrics':
        smph = SalesMetricsProcessHandler(args.local)
        smph.run_jobs()
    elif args.server == 'mpa':
        mpaph = MonthlyPremiumAttributionsProcessHandler(args.local)
        mpaph.run_jobs()
    elif args.server == 'channel-trajectory':
        mpaph = ChannelTrajectoryProcessHandler(args.local)
        mpaph.run_jobs()
    elif args.server == 'data-monitoring':
        dmph = DataMonitoringProcessHandler(args.local)
        dmph.run_jobs()

if __name__ == '__main__':
    main()
