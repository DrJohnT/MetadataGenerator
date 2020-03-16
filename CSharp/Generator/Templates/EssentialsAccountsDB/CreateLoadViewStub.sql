create view {0}.{1}Insert
as 
select {2}
from [$(EssentialsAccountsSTG)].stgQuQuantumQRE.{3} as A;
