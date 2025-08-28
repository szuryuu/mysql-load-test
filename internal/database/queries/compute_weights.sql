with queryFingerprintTotal as (
  select
    count(*) as c
  from QueryFingerprint qf
)
select
  qf2.Hash as Hash,
  count(*) as Count,
  qft.c as Total,
  cast(count(*) as decimal(10,4)) / qft.c * 100 as Weight
from QueryFingerprint qf2
cross join queryFingerprintTotal qft on 1=1
where qf2.Fingerprint not like 'insert%'
  and qf2.Fingerprint not like 'select * from rule_action%'
group by qf2.Hash
order by Weight desc
