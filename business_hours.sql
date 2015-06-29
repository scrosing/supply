with base as
(select 
    cast(t.TicketId as nvarchar(48)) as TicketId
    ,dateadd(hh, 8, t.[CreatedDate]) as [CreatedDate]
    ,dateadd(hh, 8, t.[LastUpdatedDate]) as [LastUpdatedDate]
    ,datediff(n, t.[CreatedDate], t.[LastUpdatedDate]) as SLA
    ,t.[DurationInBusinessMinutes]
	,a.Name as ActionPerformed
	,ROW_NUMBER() over(partition by t.ticketId order by t.CreatedDate) as rn
	from [dbo].[TicketActivity] t
	join [dbo].[Ticket] t0
	on t0.TicketId = t.TicketId
	join dbo.TicketActions a
	on t.ActionPerformedID = a.TicketActionID
	where t0.[CreatedDate] >= '20150601'
	and t0.TicketStatusId = 3
	and exists (select 1
	              from [dbo].[TicketActivity] ti
				  where ti.TicketId = t.TicketId
				  and ti.[DurationInBusinessMinutes] > 3000)
	)
	, valid_acitivity as(
	select TicketId, CreatedDate, LastUpdatedDate, DurationInBusinessMinutes, ActionPerformed, ROW_NUMBER() over(partition by ticketId order by CreatedDate) as rn
	from base
	where rn = 1
	or ActionPerformed not in ('SaveChanges', 'Append', 'AssignToMe')
	)
	select v1.ticketid, v1.rn, v1.LastUpdatedDate,
	       v1.ActionPerformed,
		   ((datediff(day, cast(v2.LastUpdatedDate as date), cast(v1.LastUpdatedDate as date))
		     + datepart(dw, v2.LastUpdatedDate)
		     + (7 - datepart(dw, v1.LastUpdatedDate))) / 7 * 5
		   - (case datepart(dw, v2.LastUpdatedDate) when 1 then 1 when 7 then 5 else datepart(dw, v2.LastUpdatedDate) - 1 end)
		   - (5 - case datepart(dw, v1.LastUpdatedDate) when 1 then 1 when 7 then 5 else datepart(dw, v1.LastUpdatedDate) - 1 end)) * 540
		   + case when datepart(dw, v1.LastUpdatedDate) = 7  then 540
		          when datepart(dw, v1.LastUpdatedDate) = 1 then 0 
		          when datepart(hh, v1.LastUpdatedDate) >= 18 then 540
		          when datepart(hh, v1.LastUpdatedDate) < 9 then 0
				  else (datepart(hh, v1.LastUpdatedDate) - 9) * 60 + datepart(n, v1.LastUpdatedDate) end
		   + case when datepart(dw, v2.LastUpdatedDate) = 7  then -540
		          when datepart(dw, v2.LastUpdatedDate) = 1 then 0
		          when datepart(hh, v2.LastUpdatedDate) >= 18 then -540
		          when datepart(hh, v2.LastUpdatedDate) < 9 then 0
				  else (9 - datepart(hh, v2.LastUpdatedDate)) * 60 - datepart(n, v2.LastUpdatedDate) end as estimate,

		   v1.DurationInBusinessMinutes
	from valid_acitivity v1
	left join valid_acitivity v2
	on v1.TicketId = v2.TicketId
	and v1.rn = v2.rn + 1

	 where abs(((datediff(day, cast(v2.LastUpdatedDate as date), cast(v1.LastUpdatedDate as date))
		     + datepart(dw, v2.LastUpdatedDate)
		     + (7 - datepart(dw, v1.LastUpdatedDate))) / 7 * 5
		   - (case datepart(dw, v2.LastUpdatedDate) when 1 then 1 when 7 then 5 else datepart(dw, v2.LastUpdatedDate) - 1 end)
		   - (5 - case datepart(dw, v1.LastUpdatedDate) when 1 then 1 when 7 then 5 else datepart(dw, v1.LastUpdatedDate) - 1 end)) * 540
		   + case when datepart(dw, v1.LastUpdatedDate) = 7  then 540
		          when datepart(dw, v1.LastUpdatedDate) = 1 then 0 
		          when datepart(hh, v1.LastUpdatedDate) >= 18 then 540
		          when datepart(hh, v1.LastUpdatedDate) < 9 then 0
				  else (datepart(hh, v1.LastUpdatedDate) - 9) * 60 + datepart(n, v1.LastUpdatedDate) end
		   + case when datepart(dw, v2.LastUpdatedDate) = 7  then -540
		          when datepart(dw, v2.LastUpdatedDate) = 1 then 0
		          when datepart(hh, v2.LastUpdatedDate) >= 18 then -540
		          when datepart(hh, v2.LastUpdatedDate) < 9 then 0
				  else (9 - datepart(hh, v2.LastUpdatedDate)) * 60 - datepart(n, v2.LastUpdatedDate) end - v1.DurationInBusinessMinutes) > 2
	order by 1, 2

