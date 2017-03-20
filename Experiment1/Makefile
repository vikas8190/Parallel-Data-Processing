all:
	(cp 1912.csv Programs/Sequential && cd Programs/Sequential && make run && rm 1912.csv && cd ../..)
	(cp 1912.csv Programs/NoLock && cd Programs/NoLock && make run && rm 1912.csv && cd ../..)
	(cp 1912.csv Programs/CoarseLock && cd Programs/CoarseLock && make run && rm 1912.csv && cd ../..) 
	(cp 1912.csv Programs/FineLock && cd Programs/FineLock && make run && rm 1912.csv && cd ../..) 
	(cp 1912.csv Programs/NotShared && cd Programs/NotShared && make run && rm 1912.csv && cd ../..) 
all_fibo:
	(cp 1912.csv Programs/Sequential && cd Programs/Sequential && make run_fibo && rm 1912.csv && cd ../..)
	(cp 1912.csv Programs/NoLock && cd Programs/NoLock && make run_fibo && rm 1912.csv && cd ../..)
	(cp 1912.csv Programs/CoarseLock && cd Programs/CoarseLock && make run_fibo && rm 1912.csv && cd ../..) 
	(cp 1912.csv Programs/FineLock && cd Programs/FineLock && make run_fibo && rm 1912.csv && cd ../..) 
	(cp 1912.csv Programs/NotShared && cd Programs/NotShared && make run_fibo && rm 1912.csv && cd ../..) 
clean:
	(find . -type f|grep "logs"|xargs rm -f)
	(find . -type f|grep "class"|xargs rm -f)
