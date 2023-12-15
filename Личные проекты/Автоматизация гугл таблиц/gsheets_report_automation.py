import pandas as pd
import numpy as np
import gspread
import time
from tqdm import tqdm
import re
import clickhouse_connect
import datetime

# Подключение к бд

host = ''
port = 
user = ''
password = ''

client = clickhouse_connect.get_client(host=host, port=port, username=user, password=password)

# Подключение к гугл таблицам
gc = gspread.oauth()
sh = gc.open_by_key("")
worksheet = sh.worksheet("")

# Отчётный месяц
start_date = '01.11.2023'

# Нахождение ячеек с нужной датой
value_re = re.compile(re.escape(start_date))
cells_date = worksheet.findall(value_re)

start_row = cells_date[0].row
start_column = cells_date[0].col

start_date_dt = datetime.datetime.strptime(start_date, '%d.%m.%Y')
ds = start_date_dt.strftime("%Y-%m-%d")

#######
# MAU #
#######

mau = f'''select uniqExact(user_id) as mau
from stats.sessions
where start_date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
  and service_id in (select service_id from stats.dict_services where is_active = 1)
'''

try:
    result = client.query_df(mau)

    value = int(result["mau"][0])
    worksheet.update_cell(start_row + 2, start_column, value)

except Exception as e:
    print("Error at mau all")
    print(e)

############
# MAU Pays #
############

mau_pays = f'''
select uniqExact(user_id) pays
from stats.bills
where date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
  and bill_status = 1
'''

try:
    result = client.query_df(mau_pays)

    value = int(result["pays"][0])
    worksheet.update_cell(start_row + 3, start_column, value)

except Exception as e:
    print("Error at mau pays")
    print(e)

########################
# Platforms MAU & Pays #
########################

platforms_mau_pays = f'''select *
from (
	select
		platform_group
		, 'mau' as metric
		, uniqExact(user_id) as value
	from (
		select
			service_id
			, user_id
		from stats.sessions
		where
			start_date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
			and service_id in (
				select service_id
				from stats.dict_services
				where is_active = 1
				)
		group by
			service_id
			, user_id
		)
	any left join (
		select
			service_id
			, platform_group
		from stats.dict_services
		where is_active = 1
		) using service_id
	group by platform_group
	union all
	select
		platform_group
		, 'pays' as metric
		, uniqExact(user_id) as value
	from (
		select
			date
			, user_id
			, order_id
		from stats.bills
		where
			date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
			and bill_status = 1
			and income_rub > 0
		)
	any left join (
		select
			order_id
			, platform_group
			, bills_platform
		from (
			select
				order_id
				, visitParamExtractString(order_params, 'fs_platform') as bills_platform
			from stats.transactions
			where
				transaction_date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
				and order_id > 0
			)
		any left join (
			select
				bills_platform
				, platform_group
			from stats.dict_platfroms_bills
			) using bills_platform
		) using order_id
	where platform_group <> ''
	group by platform_group
	)
order by
	platform_group desc
	, metric
'''

try:
    result = client.query_df(platforms_mau_pays)
    for i in range(len(result)):
        value = int(result["value"][i])
        worksheet.update_cell(start_row + 4 + i, start_column, value)

except Exception as e:
    print("Error at platforms mau pays")
    print(e)


#######
# DAU #
#######

dau = f'''
select round(avg(dau), 0) as dau
from (
	select
		start_date
		, uniqExact(user_id) as dau
	from stats.sessions
	where
		start_date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
		and service_id in (
			select service_id
			from stats.dict_services
			where is_active = 1
			)
	group by start_date
	)
'''

try:
    result = client.query_df(dau)

    value = int(result["dau"][0])
    worksheet.update_cell(start_row + 12, start_column, value)

except Exception as e:
    print("Error at dau all")
    print(e)

############
# DAU Pays #
############

dau_pays = f'''
select round(avg(DAU_All_pays), 0) as pays
from (
	select
		date
		, uniqExact(user_id) as DAU_All_pays
	from stats.bills
	where
		date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
		and bill_status = 1
	group by date
	)
'''

try:
    result = client.query_df(dau_pays)

    value = int(result["pays"][0])
    worksheet.update_cell(start_row + 13, start_column, value)

except Exception as e:
    print("Error at dau pays")
    print(e)

########################
# Platforms DAU & Pays #
########################

platforms_dau_pays = f'''
select *
from (
	select platform_group
		, 'dau' as metric
		, round(avg(DAU_platforms), 0) as value
	from (
		select platform_group
			, start_date
			, uniqExact(user_id) DAU_platforms
		from (
			select start_date
				, service_id
				, user_id
			from stats.sessions
			where start_date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
				and service_id in (
					select service_id
					from stats.dict_services
					where is_active = 1
					)
			group by service_id
				, start_date
				, user_id
			) any
		left join (
			select service_id
				, platform_group
			from stats.dict_services
			where is_active = 1
			) using service_id
		group by platform_group
			, start_date
		)
	group by platform_group

	union all

	select platform_group
		, 'pays' as metric
		, round(avg(DAU_platfrom_pays), 0) as value
	from (
		select date
			, platform_group
			, uniqExact(user_id) DAU_platfrom_pays
		from (
			select date
				, user_id
				, order_id
			from stats.bills
			where date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
				and bill_status = 1
			) any
		left join (
			select order_id
				, platform_group
			from (
				select order_id
					, visitParamExtractString(order_params, 'fs_platform') bills_platform
				from stats.transactions
				where transaction_date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
				) any
			left join (
				select bills_platform
					, platform_group
				from stats.dict_platfroms_bills
				) using bills_platform
			) using order_id
		group by platform_group
			, date
		)
	where platform_group <> ''
	group by platform_group
	)
order by platform_group desc
	, metric
'''

try:
    result = client.query_df(platforms_dau_pays)
    for i in range(len(result)):
        value = int(result["value"][i])
        worksheet.update_cell(start_row + 14 + i, start_column, value)

except Exception as e:
    print("Error at platforms dau pays")
    print(e)

##################
# Platforms Regs #
##################

platforms_regs = f'''
select
	platform_group
	, uniqExact(user_id) as value
from (
	select distinct
		service_id
		, user_id
	from users.first_sessions
	where
		start_date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
		and level = 1
	) any
left join (
	select
		service_id
		, platform_group
	from stats.dict_services
	) using service_id
where platform_group <> 'smart'
group by platform_group
order by platform_group desc
'''

try:
    result = client.query_df(platforms_regs)
    for i in range(len(result)):
        value = int(result["value"][i])
        if str(result["platform_group"][i]) == 'web':
            worksheet.update_cell(start_row + 22, start_column, value)
        else:
            worksheet.update_cell(start_row + 24 + i, start_column, value)

except Exception as e:
    print("Error at platforms regs")
    print(e)


##############
# ARPU ARPPU #
##############

arppu = f'''
select
	platform_group
	, round(sum_rub / MAU_platforms, 2) ARPU
	, round(sum_rub / spend_users, 2) ARPPU
from (
	select *
	from (
		select
			platform_group
			, uniqExact(user_id) MAU_platforms
		from (
			select distinct
				service_id
				, user_id
			from stats.sessions
			where start_date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
				and service_id in (
					select service_id
					from stats.dict_services
					where is_active = 1
					)
			) any
		left join (
			select
				service_id
				, platform_group
			from stats.dict_services
			where is_active = 1
			) using service_id
		group by platform_group
		) any
	left join (
		select
			platform_group
			, uniqExact(user_id) spend_users
		from (
			select distinct
				visitParamExtractString(order_params, 'fs_platform') bills_platform
				, user_id
			from stats.transactions
			where
				transaction_date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
				and amount_fm < 0
				and visitParamExtractString(order_params, 'fs_platform') <> ''
			) any
		left join (
			select
				bills_platform
				, platform_group
			from stats.dict_platfroms_bills
			) using bills_platform
		group by platform_group
		) using platform_group
	) any
left join (
	select
		platform_group
		, sum(rub) sum_rub
		, uniqExact(user_id) pays_users
	from (
		select
			bills_platform
			, rub
			, user_id
		from (
			select order_id
				, income_rub / 100 rub
				, user_id
			from stats.bills
			where date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
				and bill_status = 1
			) any
		left join (
			select distinct order_id
				, visitParamExtractString(order_params, 'fs_platform') bills_platform
			from stats.transactions
			where transaction_date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
			) using order_id
		) any
	left join (
		select bills_platform
			, platform_group
		from stats.dict_platfroms_bills
		) using bills_platform
	group by platform_group
	) using platform_group
where platform_group in ('web', 'smart')
order by platform_group desc
'''

try:
    result = client.query_df(arppu)
    for i in range(len(result)):
        value = float(result["ARPPU"][i])
        worksheet.update_cell(start_row + 32 + i, start_column, value)
    for i in range(len(result)):
        value = float(result["ARPU"][i])
        worksheet.update_cell(start_row + 35 + i, start_column, value)

except Exception as e:
    print("Error at ARPU ARPPU")
    print(e)

################
# LTV 30 InApp #
################

ltv30inapp = f'''
with 29 as days
select platform_group
	, uniqExact(user_id) users
	, round(sum(user_rub), 2) all_rub
	, round(all_rub / users, 2) ltv
from (
	select platform_group
		, user_id
		, sumIf(rub,
				(date - start_date) <= days
				and (date - start_date) >= 0) as user_rub
	from (
		select distinct user_id
			, start_date
			, platform_group
		from (
			select distinct toUInt32(user_id) user_id
				, service_id
				, start_date
			from users.first_sessions
			where
				level = 1
				and start_date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
			) any
		left join (
			select service_id
				, platform_group
			from stats.dict_services
			) using service_id
		group by user_id
			, start_date
			, platform_group
		) all
	left join (
		select toUInt32(user_id) user_id
			, date
			, sum(income_rub / 100) rub
		from stats.bills
		where
			date between '{ds}' and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
			and bill_status = 1
		group by user_id
			, date
		) using user_id
	group by user_id
		, platform_group
	)
where platform_group in ('android')
group by platform_group
'''

try:
    result = client.query_df(ltv30inapp)

    value = float(result["ltv"][0])
    worksheet.update_cell(start_row + 39, start_column, value)

except Exception as e:
    print("Error at LTV 30 InApp")
    print(e)


################
# LTV 60 InApp #
################

ltv60inapp = f'''
with 59 as days
select platform_group
	, uniqExact(user_id) users
	, round(sum(user_rub), 2) all_rub
	, round(all_rub / users, 2) ltv
from (
	select platform_group
		, user_id
		, sumIf(rub,
				(date - start_date) <= days
				and (date - start_date) >= 0) as user_rub
	from (
		select distinct user_id
			, start_date
			, platform_group
		from (
			select distinct toUInt32(user_id) user_id
				, service_id
				, start_date
			from users.first_sessions
			where
				level = 1
				and start_date between date_sub(cast('{ds}' as date), interval 1 month) and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
			) any
		left join (
			select service_id
				, platform_group
			from stats.dict_services
			) using service_id
		group by user_id
			, start_date
			, platform_group
		) all
	left join (
		select toUInt32(user_id) user_id
			, date
			, sum(income_rub / 100) rub
		from stats.bills
		where
			date between date_sub(cast('{ds}' as date), interval 1 month) and date_sub(date_add(cast('{ds}' as date), interval 1 month), interval 1 day)
			and bill_status = 1
		group by user_id
			, date
		) using user_id
	group by user_id
		, platform_group
	)
where platform_group in ('android')
group by platform_group
'''

try:
    result = client.query_df(ltv60inapp)

    value = float(result["ltv"][0])
    worksheet.update_cell(start_row + 42, start_column, value)

except Exception as e:
    print("Error at LTV 60 InApp")
    print(e)

#### Исходные данные для бюджета ####

sh = gc.open_by_key("")
worksheet = sh.worksheet("data")

# Нахождение ячеек с нужной датой
value_re = re.compile(re.escape(start_date))
cells_date = worksheet.findall(value_re)

start_row_budget = cells_date[0].row
start_column_budget = cells_date[0].col

query = open('query_budget.txt', 'r').read()

try:
    result = client.query_df(query.format(all_platforms = 1, month_stat = ds))
    for i in tqdm(range(2, len(result.columns))):
        value = float(result.iloc[0, i])
        worksheet.update_cell(start_row_budget + 4, start_column_budget + i, value)
        time.sleep(1)

except Exception as e:
    print("Error at budget all")
    print(e)

try:
    result2 = client.query_df(query.format(all_platforms = 0, month_stat = ds))
    for i in tqdm(range(2, len(result2.columns))):
        for j in range(len(result2)):
            value = float(result2.iloc[j, i])
            worksheet.update_cell(start_row_budget + j, start_column_budget + i, value)
            time.sleep(1)

except Exception as e:
    print("Error at budget platforms")
    print(e)
