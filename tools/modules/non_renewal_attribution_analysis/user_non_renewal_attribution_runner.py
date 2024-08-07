from tools.modules.non_renewal_attribution_analysis.user_non_renewal_attribution import UserNonRenewalAttribution
from utils.components.backfill_by_dt import backfill_by_date
backfill_date = backfill_by_date(latest_date='2022-01-19', earliest_date='2021-04-02')
for date in backfill_date:
    unra = UserNonRenewalAttribution(target_date=date)
    unra.execute()


