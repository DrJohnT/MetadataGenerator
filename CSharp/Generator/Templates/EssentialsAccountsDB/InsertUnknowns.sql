 
if not exists(select 1 from [{0}].[{1}] where {2} = {5})
begin
	{6}set identity_insert [{0}].[{1}] on;  

	insert into [{0}].[{1}] ({3})
	values ({4});

	{6}set identity_insert [{0}].[{1}] off;  
end
