begin
	set nocount on
		if OBJECT_ID('tempdb..##tempasd') is not null
			begin
				drop table ##tempasd
			end

			;with b as(
				SELECT TOP 1 '' as NodeName, [Document] as ContactXML, -1 as LevelId,  -1 as ParentId, -1 as NodeId, ASDDocumentId as DocumentId, a.DataSourceId
				FROM  a

			)
			,r as
			(	select c.query('local-name(.)').value('.', 'nvarchar(255)') as NodeName, DocumentId, DataSourceId,
				c.query('.') as ContactXML,
				LevelId,
				c.query('local-name(.)').value('.', 'nvarchar(255)') as NodePath,
				1 as rn
				from b
				cross apply b.ContactXML.nodes('/child::node()') as t1(c)
				union all
				select c.query('local-name(.)').value('.', 'nvarchar(255)') as NodeName, DocumentId, DataSourceId,
				c.query('.') as ContactXML,
				r.LevelId + 1 as LevelId,
				cast(r.NodePath + '/' + c.query('local-name(.)').value('.', 'nvarchar(255)') as nvarchar(255)),
				cast(ROW_NUMBER() over(partition by c.query('local-name(.)').value('.', 'nvarchar(255)'), LevelId order by (select 1)) as int)
				from r
				cross apply r.ContactXML.nodes('(./child::node())[local-name(.) = sql:column("r.NodeName")]/child::node()') as t1(c)
				where r.rn = 1
			)
			,
			r_post as
			(select *
				from r
				where rn = 1
				and r.NodeName != '')

			, r2 as
			(
				select NodeId, NodeId as N2, LevelId, LevelId as L2, NodeName as NodePath, DataSourceID
				from [Config].[XMLNodeHierarchy] (nolock) n
				where levelId = -1
				union all
				select r2.NodeId, n.NodeId as N2, r2.LevelId, n.LevelID as L2, cast(r2.NodePath + '/' + n.NodeName as nvarchar(255)), r2.DataSourceID
				from [Config].[XMLNodeHierarchy] (nolock) n
				join r2
				on n.ParentID = r2.N2
				and n.LevelId = r2.L2 + 1
			)


			select d.DataSourceName, r.NodePath, 'Added' as [Status]
			into ##tempasd
			from r_post r
			join Dim.DataSource d with (nolock)
			on r.DataSourceId = d.DataSourceId
			left join r2
			on r.LevelId = r2.L2
			and r.NodePath = r2.NodePath
			where r2.NodePath is null
			union all
			select d.DataSourceName, r2.NodePath, 'Deleted' as [Status]
			from r_post r
			right join r2
			on r.LevelId = r2.L2
			and r.NodePath = r2.NodePath
			join Dim.DataSource d with (nolock)
			on r2.DataSourceId = d.DataSourceId
			where r.NodePath is null

			if @@ROWCOUNT > 0
				begin
					EXEC msdb.dbo.sp_send_dbmail
					@profile_name = 'SQL Service Account',
					@recipients = '',
					@query = 'SELECT * from ##tempasd',
					@subject = 'XML Schema change in ASD',
					@attach_query_result_as_file = 1 ;
				end
				return 0

			end



