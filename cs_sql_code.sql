--0
--Table record counts & distinct client counts
select count(*) from dbo.df_final_demo;
select count(distinct client_id) as client_ct_dis from dbo.df_final_demo;

select count(*) from dbo.df_final_experiment_clients;
select variation, count(distinct client_id) as client_ct_dis from dbo.df_final_experiment_clients group by variation;

select count(*) from dbo.df_final_web_data_pt_1;
select count(distinct client_id) as client_ct_dis from dbo.df_final_web_data_pt_1;

select count(*) from dbo.df_final_web_data_pt_2;
select count(distinct client_id) as client_ct_dis from dbo.df_final_web_data_pt_2;


--1
--Union web data, variation data, and demographic data; select time frame for measurement.
create view dbo.full_web_data as
	select q1.client_id,
		q1.visitor_id,
		q1.visit_id,
		q1.process_step,
		case 	when q1.process_step = 'start' 		then 1
				when q1.process_step = 'step_1' 	then 2 
				when q1.process_step = 'step_2' 	then 3 
				when q1.process_step = 'step_3' 	then 4 
				when q1.process_step = 'confirm' 	then 5 else 0 end as step_nbr,
		rank() over (partition by q1.visit_id order by q1.date_time asc) as visit_rank,
		q1.date_time,
		cast(q1.date_time as date) as dte,
		q2.variation,
		q3.clnt_tenure_yr,
		q3.clnt_tenure_mnth,
		q3.clnt_age,
		q3.gendr,
		q3.num_accts,
		q3.bal,
		q3.calls_6_mnth,
		q3.logons_6_mnth
		from (
			select t1.client_id,
				t1.visitor_id,
				t1.visit_id,
				t1.process_step,
				t1.date_time
			from dbo.df_final_web_data_pt_1 t1
		
			union all
			
			select t2.client_id,
				t2.visitor_id,
				t2.visit_id,
				t2.process_step,
				t2.date_time
			from dbo.df_final_web_data_pt_2 t2
			) q1
		
		left join dbo.df_final_experiment_clients q2
		on q1.client_id = q2.client_id
		
		left join dbo.df_final_demo q3
		on q1.client_id = q3.client_id
		
		where 	cast(q1.date_time as date) >= '2017-03-15'
			and cast(q1.date_time as date) <= '2017-04-30'
	;

--Checks records in union table;
select count(*) from dbo.full_web_data;
select count(distinct client_id) as client_ct_dis from dbo.full_web_data;
select variation, count(distinct client_id) as client_ct_dis from dbo.full_web_data group by variation;
select min(dte), max(dte) from dbo.full_web_data;	--Date range: 3/15/2017 through 6/20/2017
select distinct client_id from dbo.full_web_data where clnt_age is null and variation in ('Control', 'Test');

--Identifies visitor_ids that have more than 1 client_id
create view dbo.full_web_data_2 as
	select distinct q1.visitor_id
	from (select distinct q1.visitor_id,
			count(distinct client_id) as client_ct
		from dbo.full_web_data q1
		group by visitor_id
		) q1
	where client_ct > 1
	;

select count(distinct visitor_id) as visitor_id_ct_dis from dbo.full_web_data_2;

--2
--Removes NA and null variations from web_data & removes records where client demographics is not available
drop view dbo.web_data_2;
create view dbo.web_data_2 as
	select client_id,
		visitor_id,
		visit_id,
		process_step,
		visit_rank,
		date_time,
		dte,
		variation,
		clnt_tenure_yr,
		clnt_tenure_mnth,
		clnt_age,
		gendr,
		num_accts,
		bal,
		calls_6_mnth,
		logons_6_mnth
		from dbo.full_web_data
		where variation in ('Control', 'Test') and 
			clnt_age is not null
			--and visitor_id not in (select visitor_id from dbo.full_web_data_2)
	;

--Checks records in union table;
select count(*) from dbo.web_data_2;
select count(distinct client_id) as client_ct_dis from dbo.web_data_2;
select variation, count(distinct client_id) as client_ct_dis from dbo.web_data_2 group by variation;

--Summary Statistics for Variations
create view dbo.summary_stats as
select t1.variation,
	'distinct count' as summary_stat,
	count(distinct t1.client_id) as client_dis_ct,
	''		as clnt_tenure_yr,
	''		as clnt_age,
	''		as num_accts,
	''			as bal,
	''		as calls_6_mnth,
	''		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.web_data_2
		) t1
	group by t1.Variation
union all
select t1.variation,
	'minimum' as summary_stat,
	''							 as client_dis_ct,
	MIN(t1.clnt_tenure_yr)		as clnt_tenure_yr,
	MIN(t1.clnt_age)			as clnt_age,
	MIN(t1.num_accts)			as num_accts,
	MIN(t1.bal)					as bal,
	MIN(t1.calls_6_mnth)		as calls_6_mnth,
	MIN(t1.logons_6_mnth)		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.web_data_2
		) t1
	group by t1.Variation
union all
select t1.variation,
	'average' as summary_stat,
	''							 as client_dis_ct,
	AVG(t1.clnt_tenure_yr)		as clnt_tenure_yr,
	AVG(t1.clnt_age)			as clnt_age,
	AVG(t1.num_accts)			as num_accts,
	AVG(t1.bal)					as bal,
	AVG(t1.calls_6_mnth)		as calls_6_mnth,
	AVG(t1.logons_6_mnth)		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.web_data_2
		) t1
	group by t1.Variation
union all
select t1.variation,
	'maximum' as summary_stat,
	''							 as client_dis_ct,
	max(t1.clnt_tenure_yr)		as clnt_tenure_yr,
	max(t1.clnt_age)			as clnt_age,
	max(t1.num_accts)			as num_accts,
	max(t1.bal)					as bal,
	max(t1.calls_6_mnth)		as calls_6_mnth,
	max(t1.logons_6_mnth)		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.web_data_2
		) t1
	group by t1.Variation
union all
select t1.variation,
	'stdev' as summary_stat,
	''							 as client_dis_ct,
	stdev(t1.clnt_tenure_yr)		as clnt_tenure_yr,
	stdev(t1.clnt_age)			as clnt_age,
	stdev(t1.num_accts)			as num_accts,
	stdev(t1.bal)					as bal,
	stdev(t1.calls_6_mnth)		as calls_6_mnth,
	stdev(t1.logons_6_mnth)		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.web_data_2
		) t1
	group by t1.Variation
;
select * from dbo.summary_stats;

--3
--Funnel by client - visitor id - visit id
drop view dbo.funnel_1;
create view dbo.funnel_1 as
	select distinct t1.client_id,
		t1.visitor_id,
		t1.visit_id,
		t1.dte,
		t1.variation,
		t1.clnt_tenure_yr,
		t1.clnt_tenure_mnth,
		t1.clnt_age,
		t1.gendr,
		t1.num_accts,
		t1.bal,
		t1.calls_6_mnth,
		t1.logons_6_mnth,
		sum(case when t1.process_step = 'start' 	then 1 else 0 end) as p1_start,
		sum(case when t1.process_step = 'step_1' 	then 1 else 0 end) as p2_step1,
		sum(case when t1.process_step = 'step_2' 	then 1 else 0 end) as p3_step2,
		sum(case when t1.process_step = 'step_3' 	then 1 else 0 end) as p4_step3,
		sum(case when t1.process_step = 'confirm' 	then 1 else 0 end) as pg5_confirm
	from dbo.web_data_2 t1
	group by t1.client_id,
		t1.visitor_id,
		t1.visit_id,
		t1.dte,
		t1.variation,
		t1.clnt_tenure_yr,
		t1.clnt_tenure_mnth,
		t1.clnt_age,
		t1.gendr,
		t1.num_accts,
		t1.bal,
		t1.calls_6_mnth,
		t1.logons_6_mnth
;

--Checks records in union table;
select count(*) from dbo.funnel_1;
select count(distinct client_id) as client_ct_dis from dbo.funnel_1;
select variation, count(distinct client_id) as client_ct_dis from dbo.funnel_1 group by variation;


--4
--Funnel by client - visitor id - visit id - defines a complete process
create view dbo.funnel_2 as
	select distinct t1.client_id,
		t1.visitor_id,
		t1.visit_id,
		t1.dte,
		t1.variation,
		t1.clnt_tenure_yr,
		t1.clnt_age,
		t1.num_accts,
		t1.bal,
		t1.calls_6_mnth,
		t1.logons_6_mnth,
		t1.p1_start,
		t1.p2_step1,
		t1.p3_step2,
		t1.p4_step3,
		t1.pg5_confirm,
		case when t1.p1_start > 0 then '1' else 0 end as cmpltd_p1_start,
		case when t1.p1_start > 0 and t1.p2_step1 > 0 then '1' else 0 end as cmpltd_p2_step1,
		case when t1.p1_start > 0 and t1.p2_step1 > 0 and t1.p3_step2 > 0 then '1' else 0 end as cmpltd_p3_step2,
		case when t1.p1_start > 0 and t1.p2_step1 > 0 and t1.p3_step2 > 0 and t1.p4_step3 > 0 then '1' else 0 end as cmpltd_p4_step3,
		case when t1.p1_start > 0 and t1.p2_step1 > 0 and t1.p3_step2 > 0 and t1.p4_step3 > 0 and t1.pg5_confirm > 0 then '1' else 0 end as cmpltd_pg5_confirm
	from dbo.funnel_1 t1
;

--5
--Roll up the funnel for the measurement period
create view dbo.funnel_pivot as
	select distinct t1.client_id,
		t1.dte,
		t1.variation,
		t1.clnt_tenure_yr,
		t1.clnt_age,
		t1.num_accts,
		t1.bal,
		t1.calls_6_mnth,
		t1.logons_6_mnth,
		'1-start' as pg,
		sum(cmpltd_p1_start) as sum_sessions
	from dbo.funnel_2 t1
	where t1.cmpltd_p1_start = 1
	group by t1.client_id, t1.dte, t1.variation, t1.clnt_tenure_yr, t1.clnt_age, t1.num_accts, t1.bal, t1.calls_6_mnth, t1.logons_6_mnth
union all
	select distinct t1.client_id,
		t1.dte,
		t1.variation,
		t1.clnt_tenure_yr,
		t1.clnt_age,
		t1.num_accts,
		t1.bal,
		t1.calls_6_mnth,
		t1.logons_6_mnth,
		'2-step1' as pg,
		sum(cmpltd_p2_step1) as sum_sessions
	from dbo.funnel_2 t1
	where t1.cmpltd_p2_step1 = 1
	group by t1.client_id, t1.dte, t1.variation, t1.clnt_tenure_yr, t1.clnt_age, t1.num_accts, t1.bal, t1.calls_6_mnth, t1.logons_6_mnth
union all
	select distinct t1.client_id,
		t1.dte,
		t1.variation,
		t1.clnt_tenure_yr,
		t1.clnt_age,
		t1.num_accts,
		t1.bal,
		t1.calls_6_mnth,
		t1.logons_6_mnth,
		'3-step2' as pg,
		sum(cmpltd_p3_step2) as sum_sessions
	from dbo.funnel_2 t1
	where t1.cmpltd_p3_step2 = 1
	group by t1.client_id, t1.dte, t1.variation, t1.clnt_tenure_yr, t1.clnt_age, t1.num_accts, t1.bal, t1.calls_6_mnth, t1.logons_6_mnth
union all
	select distinct t1.client_id,
		t1.dte,
		t1.variation,
		t1.clnt_tenure_yr,
		t1.clnt_age,
		t1.num_accts,
		t1.bal,
		t1.calls_6_mnth,
		t1.logons_6_mnth,
		'4-step3' as pg,
		sum(cmpltd_p4_step3) as sum_sessions
	from dbo.funnel_2 t1
	where t1.cmpltd_p4_step3 = 1
	group by t1.client_id, t1.dte, t1.variation, t1.clnt_tenure_yr, t1.clnt_age, t1.num_accts, t1.bal, t1.calls_6_mnth, t1.logons_6_mnth
union all
	select distinct t1.client_id,
		t1.dte,
		t1.variation,
		t1.clnt_tenure_yr,
		t1.clnt_age,
		t1.num_accts,
		t1.bal,
		t1.calls_6_mnth,
		t1.logons_6_mnth,
		'5-confirm' as pg,
		sum(cmpltd_pg5_confirm) as sum_sessions
	from dbo.funnel_2 t1
	where t1.cmpltd_pg5_confirm = 1
	group by t1.client_id, t1.dte, t1.variation, t1.clnt_tenure_yr, t1.clnt_age, t1.num_accts, t1.bal, t1.calls_6_mnth, t1.logons_6_mnth
;

--6
--Final table for full measurement period
drop view dbo.funnel_final_full_period;
create view dbo.funnel_final_full_period as
select q1.variation,
	q1.pg,
	q1.sum_sessions,
	lag(q1.sum_sessions, 1, 0) over (partition by q1.variation order by q1.pg asc) as sum_sessions_prior_step,
	q2.sum_sessions as sum_sessions_startpg
	from (
		select distinct t1.variation,
				t1.pg,
				sum(t1.sum_sessions) as sum_sessions
		from dbo.funnel_pivot t1
		group by t1.variation,
				t1.pg
		) q1
	left join (
		select distinct t1.variation,
				sum(t1.sum_sessions) as sum_sessions
		from dbo.funnel_pivot t1
		where t1.pg = '1-start'
		group by t1.variation
		) q2
	on q1.Variation = q2.Variation
	group by q1.variation,
	q1.pg,
	q1.sum_sessions,
	q2.sum_sessions
;


--7
--Final table for full measurement period
create view dbo.funnel_final_daily as
select q1.dte,
	q1.variation,
	q1.pg,
	q1.sum_sessions,
	lag(q1.sum_sessions, 1, 0) over (partition by q1.dte, q1.variation order by q1.pg asc) as sum_sessions_prior_step,
	q2.sum_sessions as sum_sessions_startpg
	from (
		select distinct t1.dte,
				t1.variation,
				t1.pg,
				sum(t1.sum_sessions) as sum_sessions
		from dbo.funnel_pivot t1
		group by t1.dte, t1.variation, t1.pg
		) q1
	left join (
		select distinct t1.dte,
				t1.variation,
				sum(t1.sum_sessions) as sum_sessions
		from dbo.funnel_pivot t1
		where t1.pg = '1-start'
		group by t1.dte, t1.variation
		) q2
	on q1.Variation = q2.Variation
	and q1.dte = q2.dte
	group by q1.dte,
	q1.variation,
	q1.pg,
	q1.sum_sessions,
	q2.sum_sessions
;

--8
--Customer stats for start of application
--Summary Statistics for Variations
create view dbo.summary_stats_start as
select t1.variation,
	'start' as pg,
	'distinct count' as summary_stat,
	count(distinct t1.client_id) as client_dis_ct,
	''		as clnt_tenure_yr,
	''		as clnt_age,
	''		as num_accts,
	''			as bal,
	''		as calls_6_mnth,
	''		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.funnel_pivot
		where pg = '1-start'
		) t1
	group by t1.variation
union all
select t1.variation,
	'start' as pg,
	'minimum' as summary_stat,
	''							 as client_dis_ct,
	MIN(t1.clnt_tenure_yr)		as clnt_tenure_yr,
	MIN(t1.clnt_age)			as clnt_age,
	MIN(t1.num_accts)			as num_accts,
	MIN(t1.bal)					as bal,
	MIN(t1.calls_6_mnth)		as calls_6_mnth,
	MIN(t1.logons_6_mnth)		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.funnel_pivot
		where pg = '1-start'
		) t1
	group by t1.variation
union all
select t1.variation,
	'start' as pg,
	'average' as summary_stat,
	''							 as client_dis_ct,
	AVG(t1.clnt_tenure_yr)		as clnt_tenure_yr,
	AVG(t1.clnt_age)			as clnt_age,
	AVG(t1.num_accts)			as num_accts,
	AVG(t1.bal)					as bal,
	AVG(t1.calls_6_mnth)		as calls_6_mnth,
	AVG(t1.logons_6_mnth)		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.funnel_pivot
		where pg = '1-start'
		) t1
	group by t1.variation
union all
select t1.variation,
	'start' as pg,
	'maximum' as summary_stat,
	''							 as client_dis_ct,
	max(t1.clnt_tenure_yr)		as clnt_tenure_yr,
	max(t1.clnt_age)			as clnt_age,
	max(t1.num_accts)			as num_accts,
	max(t1.bal)					as bal,
	max(t1.calls_6_mnth)		as calls_6_mnth,
	max(t1.logons_6_mnth)		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.funnel_pivot
		where pg = '1-start'
		) t1
	group by t1.variation
union all
select t1.variation,
	'start' as pg,
	'stdev' as summary_stat,
	''							 as client_dis_ct,
	stdev(t1.clnt_tenure_yr)		as clnt_tenure_yr,
	stdev(t1.clnt_age)			as clnt_age,
	stdev(t1.num_accts)			as num_accts,
	stdev(t1.bal)					as bal,
	stdev(t1.calls_6_mnth)		as calls_6_mnth,
	stdev(t1.logons_6_mnth)		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.funnel_pivot
		where pg = '1-start'
		) t1
	group by t1.variation
;

--9
--Customer stats for confirm page of application
--Summary Statistics for Variations
create view dbo.summary_stats_confirm as
select t1.variation,
	'confirm' as pg,
	'distinct count' as summary_stat,
	count(distinct t1.client_id) as client_dis_ct,
	''		as clnt_tenure_yr,
	''		as clnt_age,
	''		as num_accts,
	''			as bal,
	''		as calls_6_mnth,
	''		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.funnel_pivot
		where pg = '5-confirm'
		) t1
	group by t1.variation
union all
select t1.variation,
	'confirm' as pg,
	'minimum' as summary_stat,
	''							 as client_dis_ct,
	MIN(t1.clnt_tenure_yr)		as clnt_tenure_yr,
	MIN(t1.clnt_age)			as clnt_age,
	MIN(t1.num_accts)			as num_accts,
	MIN(t1.bal)					as bal,
	MIN(t1.calls_6_mnth)		as calls_6_mnth,
	MIN(t1.logons_6_mnth)		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.funnel_pivot
		where pg = '5-confirm'
		) t1
	group by t1.variation
union all
select t1.variation,
	'confirm' as pg,
	'average' as summary_stat,
	''							 as client_dis_ct,
	AVG(t1.clnt_tenure_yr)		as clnt_tenure_yr,
	AVG(t1.clnt_age)			as clnt_age,
	AVG(t1.num_accts)			as num_accts,
	AVG(t1.bal)					as bal,
	AVG(t1.calls_6_mnth)		as calls_6_mnth,
	AVG(t1.logons_6_mnth)		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.funnel_pivot
		where pg = '5-confirm'
		) t1
	group by t1.variation
union all
select t1.variation,
	'confirm' as pg,
	'maximum' as summary_stat,
	''							 as client_dis_ct,
	max(t1.clnt_tenure_yr)		as clnt_tenure_yr,
	max(t1.clnt_age)			as clnt_age,
	max(t1.num_accts)			as num_accts,
	max(t1.bal)					as bal,
	max(t1.calls_6_mnth)		as calls_6_mnth,
	max(t1.logons_6_mnth)		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.funnel_pivot
		where pg = '5-confirm'
		) t1
	group by t1.variation
union all
select t1.variation,
	'confirm' as pg,
	'stdev' as summary_stat,
	''							 as client_dis_ct,
	stdev(t1.clnt_tenure_yr)		as clnt_tenure_yr,
	stdev(t1.clnt_age)			as clnt_age,
	stdev(t1.num_accts)			as num_accts,
	stdev(t1.bal)					as bal,
	stdev(t1.calls_6_mnth)		as calls_6_mnth,
	stdev(t1.logons_6_mnth)		as logons_6_mnth
	from (select distinct client_id,variation, clnt_tenure_yr, clnt_age, num_accts, bal, calls_6_mnth, logons_6_mnth
		from dbo.funnel_pivot
		where pg = '5-confirm'
		) t1
	group by t1.variation
;

--10
--Identifies steps by timestamp for client - visitor - visit
drop view dbo.funnel_1_tmstmp;
create view dbo.funnel_1_tmstmp as
--START 5TH QUERY FOR PAGE = CONFIRM
select t7.*
		,t8.process_step as confirm_pg
		,t8.visit_rank as visit_rank_pg5
		,t8.date_time as confirm_time
from (	

	--START 4TH QUERY FOR PAGE = STEP_3
	select t5.*
			,t6.process_step as step3_pg
			,t6.visit_rank as visit_rank_pg4
	from (
			--START 3RD QUERY FOR PAGE = STEP_2
			select t3.*
					,t4.process_step as step2_pg
					,t4.visit_rank as visit_rank_pg3
			from (

					--START 2ND QUERY FOR PAGE = STEP_1
					select t1.*
						,t2.process_step as step1_pg
						,t2.visit_rank as visit_rank_pg2
						from (	

							--START 1ST QUERY FOR PAGE = START
							select q1.client_id,
									q1.variation,
									q1.visitor_id,
									q1.visit_id,
									q1.dte,
									q1.clnt_tenure_yr,
									q1.clnt_age,
									q1.num_accts,
									q1.bal,
									q1.calls_6_mnth,
									q1.logons_6_mnth,
									q1.visit_rank,
									q1.date_time as start_time,
									q1.process_step as start_pg			
							from dbo.web_data_2 q1
							where --client_id = 117032
							/*and*/ process_step = 'start'
							--END 1ST QUERY FOR PAGE = START

						) t1
						
						left join dbo.web_data_2 t2
						on t1.client_id = t2.client_id
							and t1.visitor_id = t2.visitor_id
							and t1.visit_id = t2.visit_id
							and t2.visit_rank = (t1.visit_rank + 1)
							and t2.process_step = 'step_1'	
					--END 2ND QUERY FOR PAGE = STEP_1

				) t3

				left join dbo.web_data_2 t4
				on t3.client_id = t4.client_id
					and t3.visitor_id = t4.visitor_id
					and t3.visit_id = t4.visit_id
					and t4.visit_rank = (t3.visit_rank_pg2 + 1)
					and t4.process_step = 'step_2'	
			--END 3RD QUERY FOR PAGE = STEP_3
		) t5

	left join dbo.web_data_2 t6
	on t5.client_id = t6.client_id
		and t5.visitor_id = t6.visitor_id
		and t5.visit_id = t6.visit_id
		and t6.visit_rank = (t5.visit_rank_pg3 + 1)
		and t6.process_step = 'step_3'	
	--END 4TH QUERY FOR PAGE = STEP_3

	) t7

left join dbo.web_data_2 t8
on t7.client_id = t8.client_id
	and t7.visitor_id = t8.visitor_id
	and t7.visit_id = t8.visit_id
	and t8.visit_rank = (t7.visit_rank_pg4 + 1)
	and t8.process_step = 'confirm'	
--END 5TH QUERY FOR PAGE = CONFIRM
;

--11
--Calculates time to completion
select t1.variation,
	AVG(t1.time_to_completion) as avg_time_to_completion,
	STDEV(t1.time_to_completion) as stdev_time_to_completion
from (
		select q1.client_id,
			q1.variation,
			q1.visitor_id,
			q1.visit_id,
			q1.dte,
			q1.clnt_tenure_yr,
			q1.clnt_age,
			q1.num_accts,
			q1.bal,
			q1.calls_6_mnth,
			q1.logons_6_mnth,
			q1.visit_rank,
			q1.start_time,
			q1.start_pg,
			q1.step1_pg,
			q1.visit_rank_pg2,
			q1.step2_pg,
			q1.visit_rank_pg3,
			q1.step3_pg,
			q1.visit_rank_pg4,
			q1.confirm_pg,
			q1.visit_rank_pg5,
			q1.confirm_time,
			(datediff(SECOND, q1.start_time, q1.confirm_time)) as time_to_completion
		from dbo.funnel_1_tmstmp q1
	) t1
group by t1.variation
;

--12
--Identifies number of times the process was started within a single visit
select t1.variation,
	AVG(t1.visit_ct) as avg_visit_ct
from (
		select q1.client_id,
			q1.variation,
			q1.visitor_id,
			count(q1.visit_id) as visit_ct
		from dbo.funnel_1_tmstmp q1
		where q1.confirm_pg is null
		and q1.start_pg is not null
		group by q1.client_id,
			q1.variation,
			q1.visitor_id
	) t1
group by t1.variation
;

--13
--Counts page views for those sessions that did not complete the process
	select client_id,
		variation,
		process_step,
		count(process_step) as process_step_ct
		from dbo.web_data_2
		where visit_id not in (select distinct visit_id from web_data_2 where process_step = 'confirm')
		group by client_id,
		variation,
		process_step
;